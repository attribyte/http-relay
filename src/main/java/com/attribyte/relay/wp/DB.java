
package com.attribyte.relay.wp;

import com.attribyte.client.ClientProtos;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.attribyte.sql.pool.ConnectionPool;
import org.attribyte.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class DB {

   DB(final ConnectionPool connectionPool,
      final String postNamespace,
      final String commentNamespace,
      final long siteId,
      final SiteMeta overrideSiteMeta) throws SQLException {
      this.connectionPool = connectionPool;
      this.postNamespace = postNamespace;
      this.commentNamespace = commentNamespace;

      this.userCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(30, TimeUnit.MINUTES) //TODO
              .build(new CacheLoader<Long, Optional<UserMeta>>() {
                 @Override
                 public Optional<UserMeta> load(final Long userId) throws SQLException {
                    return selectUser(userId);
                 }
              });

      this.termCache = CacheBuilder.newBuilder()
              .concurrencyLevel(4)
              .expireAfterWrite(30, TimeUnit.MINUTES) //TODO...
              .build(new CacheLoader<Long, Optional<TermMeta>>() {
                 @Override
                 public Optional<TermMeta> load(final Long termId) throws Exception {
                    return selectTerm(termId);
                 }
              });

      if(siteId < 2) {
         this.postsTableName = "wp_posts";
         this.commentsTableName = "wp_comments";
         this.optionsTableName = "wp_options";
         this.termsTableName = "wp_terms";
         this.termRelationshipsTableName = "wp_term_relationships";
         this.termTaxonomyTableName = "wp_term_taxonomy";
      } else {
         this.postsTableName = "wp_" + siteId + "_posts";
         this.commentsTableName = "wp_" + siteId + "_comments";
         this.optionsTableName = "wp_" + siteId + "_options";
         this.termsTableName = "wp_" + siteId + "_terms";
         this.termRelationshipsTableName = "wp_" + siteId + "_term_relationships";
         this.termTaxonomyTableName = "wp_" + siteId + "_term_taxonomy";
      }

      this.selectOptionSQL = "SELECT option_value FROM " + this.optionsTableName + " WHERE option_name=?";
      this.selectPostByIdSQL = selectPostSQL + this.postsTableName + " WHERE id=?";
      this.siteMeta = overrideSiteMeta != null ? selectSiteMeta(siteId).overrideWith(overrideSiteMeta) : selectSiteMeta(siteId);

      this.parentSite = ClientProtos.WireMessage.Site.newBuilder()
              .setUID(buildId(this.siteMeta.id))
              .setTitle(this.siteMeta.title)
              .setDescription(this.siteMeta.description)
              .setUrl(this.siteMeta.baseURL)
              .build();

      this.parentSource = ClientProtos.WireMessage.Source.newBuilder()
              .setUID(buildId(this.siteMeta.id))
              .setTitle(this.siteMeta.title)
              .setDescription(this.siteMeta.description)
              .setUrl(this.siteMeta.baseURL)
              .build();
   }

   private static final String selectPostSQL = "SELECT ID, post_author, post_date_gmt, " +
           "post_title, post_excerpt, post_content, post_status, post_modified_gmt, post_type FROM ";

   /**
    * Builds a post from a result set.
    * @param rs The result set.
    * @return The post, or {@code absent} if post was not found or not allowed.
    * @throws SQLException on database error.
    */
   private Optional<ClientProtos.WireMessage.Entry.Builder> postFromResultSet(final ResultSet rs) throws SQLException {

      String title = Strings.nullToEmpty(rs.getString(4));
      String summary = Strings.nullToEmpty(rs.getString(5));
      String content = Strings.nullToEmpty(rs.getString(6));

      ClientProtos.WireMessage.Entry.Builder builder =
              ClientProtos.WireMessage.Entry.newBuilder()
                      .setUID(buildId(rs.getLong(1)))
                      .setAuthor(ClientProtos.WireMessage.Author.newBuilder()
                              .setId(rs.getLong(2))
                              .setSourceUID(this.parentSite.getUID())
                      )
                      .setParentSite(ClientProtos.WireMessage.Site.newBuilder().setUID(this.parentSite.getUID()))
                      .setPermanent(true)
                      .setPublishTimeMillis(rs.getTimestamp(3).getTime())
                      .setLastModifiedMillis(rs.getTimestamp(8).getTime());

      if(!title.isEmpty()) {
         builder.setTitle(title);
      }

      if(!summary.isEmpty()) {
         builder.setSummary(summary);
      }

      if(!content.isEmpty()) {
         builder.setContent(content);
      }

      return Optional.of(builder);
   }

   /**
    * Selects a post by id.
    * @param id The id.
    * @return The post or {@code absent} if missing or disallowed status or type.
    * @throws SQLException on database error.
    */
   public Optional<ClientProtos.WireMessage.Entry.Builder> selectPost(final long id) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = connectionPool.getConnection();
         stmt = conn.prepareStatement(selectPostByIdSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         return rs.next() ? postFromResultSet(rs) : Optional.absent();
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /*
      In our experience, ahem, it is not uncommon for publishing systems to have long sequences
      of posts with identical modification times (because of bulk-updates, etc.)
      We'll deal with that by requiring both the modified time to select after
      and the last id we've seen.
    */

   private static final String selectModPostsPrefixSQL =
           "SELECT ID, post_modified_gmt, post_status, post_type, post_author, post_name FROM ";
   private static final String selectModPostsSuffixSQL =
           " WHERE post_modified_gmt > ? OR (post_modified_gmt = ? AND ID > ?) ORDER BY post_modified_gmt ASC, ID ASC LIMIT ?";

   /**
    * Gets metadata for posts.
    * @param maybeStart If specified, metadata changed after this will be returned.
    * @param limit The maximum number returned.
    * @return The list of metadata for modified posts.
    * @throws SQLException on database error.
    */
   public List<PostMeta> selectModifiedPosts(final Optional<PostMeta> maybeStart, final int limit) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      PostMeta start = maybeStart.or(PostMeta.ZERO);
      List<PostMeta> metaList = Lists.newArrayListWithExpectedSize(limit < 1024 ? limit : 1024);
      try {
         conn = connectionPool.getConnection();
         stmt = conn.prepareStatement(selectModPostsPrefixSQL + postsTableName + selectModPostsSuffixSQL);
         stmt.setTimestamp(1, new Timestamp(start.lastModifiedMillis));
         stmt.setTimestamp(2, new Timestamp(start.lastModifiedMillis));
         stmt.setLong(3, start.id);
         stmt.setInt(4, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            metaList.add(new PostMeta(rs.getLong(1), rs.getTimestamp(2).getTime(), rs.getString(3), rs.getString(4), rs.getLong(5), rs.getString(6)));
         }
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }

      return metaList;
   }

   /**
    * Gets term metadata from the result set.
    * @param rs The result set.
    * @return The term metadata.
    * @throws SQLException on database error.
    */
   private TermMeta termFromResultSet(final ResultSet rs) throws SQLException {
      return new TermMeta(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getLong(4));
   }

   private static final String selectTermPrefixSQL = "SELECT term_id, name, slug, term_group FROM ";
   private static final String selectTermSuffixSQL = " WHERE term_id=?";

   /**
    * Selects a term from the database.
    * @param id The term id.
    * @return The term metadata or {@code absent} if not found.
    * @throws SQLException on database error.
    */
   public Optional<TermMeta> selectTerm(final long id) throws SQLException {

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = connectionPool.getConnection();
         stmt = conn.prepareStatement(selectTermPrefixSQL + termsTableName + selectTermSuffixSQL);
         stmt.setLong(1, id);
         rs = stmt.executeQuery();
         return rs.next() ? Optional.of(termFromResultSet(rs)) : Optional.absent();
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Resolves terms for a post and specified taxonomy.
    * @param postId The post id.
    * @param taxonomy The taxonomy.
    * @return The list of term metadata.
    * @throws SQLException on database error.
    */
   public List<TermMeta> resolvePostTerms(final long postId, final String taxonomy) throws SQLException {
      List<Long> termIds = selectPostTerms(postId, taxonomy);
      List<TermMeta> terms = Lists.newArrayListWithCapacity(termIds.size());
      for(long termId : termIds) {
         Optional<TermMeta> maybeTerm = resolveTerm(termId);
         if(maybeTerm.isPresent()) {
            terms.add(maybeTerm.get());
         }
      }
      return terms;
   }

   /**
    * Selects the term ids for a post and specified taxonomy.
    * @param postId The post id.
    * @param taxonomy The taxonomy.
    * @return The list of term ids.
    * @throws SQLException on database error.
    */
   public List<Long> selectPostTerms(final long postId, final String taxonomy) throws SQLException {
      String sql = new StringBuilder("SELECT term_id FROM ")
              .append(termRelationshipsTableName).append(",").append(termTaxonomyTableName)
              .append(" WHERE object_id=? AND taxonomy=?")
              .append(" AND ")
              .append(termTaxonomyTableName).append(".term_taxonomy_id=")
              .append(termRelationshipsTableName).append(".term_taxonomy_id")
              .toString();

      List<Long> termIdList = Lists.newArrayListWithExpectedSize(4);

      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = connectionPool.getConnection();
         stmt = conn.prepareStatement(sql);
         stmt.setLong(1, postId);
         stmt.setString(2, taxonomy);
         rs = stmt.executeQuery();
         while(rs.next()) {
            termIdList.add(rs.getLong(1));
         }
         return termIdList;
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }


   /**
    * Resolves a term, possibly from cache.
    * @param termId The term id.
    * @return The term or {@code absent} if not found.
    * @throws SQLException on database error.
    */
   public Optional<TermMeta> resolveTerm(final long termId) throws SQLException {
      try {
         return termCache.get(termId);
      } catch(ExecutionException ee) {
         Throwables.propagateIfPossible(ee.getCause(), SQLException.class);
         throw new AssertionError();
      }
   }

   private static final String selectUserSQL = "SELECT ID, user_nicename, display_name FROM wp_users";

   /**
    * Builds an id with optional postNamespace.
    * @param id The numeric id.
    * @return The id.
    */
   ClientProtos.WireMessage.Id buildId(final long id) {
      return postNamespace == null ? ClientProtos.WireMessage.Id.newBuilder().setId(Long.toString(id)).build() :
              ClientProtos.WireMessage.Id.newBuilder().setId(Long.toString(id)).setNamespace(postNamespace).build();
   }

   /**
    * Creates a user from a result set.
    * @param rs The result set.
    * @return The user.
    * @throws SQLException on database error.
    */
   private UserMeta userFromResultSet(final ResultSet rs) throws SQLException {
      return new UserMeta(rs.getLong(1), rs.getString(2), rs.getString(3));
   }

   private static final String selectUserByIdSQL = selectUserSQL + " WHERE ID=?";

   /**
    * Resolves a user, possibly with the internal cache.
    * @param userId The user id.
    * @return The user or {@code absent}, if not found.
    * @throws SQLException on database error.
    */
   public Optional<UserMeta> resolveUser(final long userId) throws SQLException {
      try {
         return userCache.get(userId);
      } catch(ExecutionException ee) {
         Throwables.propagateIfPossible(ee.getCause(), SQLException.class);
         throw new AssertionError();
      }
   }

   /**
    * Selects a user from the database.
    * @param  userId The user id.
    * @return The author or {@code absent} if not found.
    * @throws SQLException on database error.
    */
   public Optional<UserMeta> selectUser(final long userId) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;
      try {
         conn = connectionPool.getConnection();
         stmt = conn.prepareStatement(selectUserByIdSQL);
         stmt.setLong(1, userId);
         rs = stmt.executeQuery();
         return rs.next() ? Optional.of(userFromResultSet(rs)) : Optional.absent();
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * Selects the site metadata from the options table.
    * @return The site metadata.
    * @throws SQLException on database error.
    */
   public SiteMeta selectSiteMeta(final long siteId) throws SQLException {
      String baseURL = getOption("home").orNull();
      String title = getOption("blogname").orNull();
      String description = getOption("blogdescription").orNull();
      String permalinkStructure = getOption("permalink_structure ").or("/?p=%postid%");
      long defaultCategoryId = Long.parseLong(getOption("default_category").or("0"));
      TermMeta defaultCategory = resolveTerm(defaultCategoryId).or(new TermMeta(0L, "Uncategorized", "uncategorized", 0L));
      return new SiteMeta(siteId, baseURL, title, description, permalinkStructure, defaultCategory);
   }

   /**
    * Gets a configuration option.
    * @param optionName The option name.
    * @return The option value or {@code absent} if not found.
    * @throws SQLException on database error.
    */
   public Optional<String> getOption(final String optionName) throws SQLException {
      Connection conn = null;
      PreparedStatement stmt = null;
      ResultSet rs = null;

      try {
         conn = connectionPool.getConnection();
         stmt = conn.prepareStatement(selectOptionSQL);
         stmt.setString(1, optionName);
         rs = stmt.executeQuery();
         if(rs.next()) {
            String val = rs.getString(1);
            return val != null ? Optional.of(val.trim()) : Optional.absent();
         } else {
            return Optional.absent();
         }
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   /**
    * The connection pool.
    */
   private final ConnectionPool connectionPool;

   /**
    * The id namespace for posts.
    */
   private final String postNamespace;

   /**
    * The id namespace for comments.
    */
   private final String commentNamespace;

   private final LoadingCache<Long, Optional<UserMeta>> userCache;
   private final LoadingCache<Long, Optional<TermMeta>> termCache;


   private final String postsTableName;
   private final String commentsTableName;
   private final String optionsTableName;
   private final String termsTableName;
   private final String termRelationshipsTableName;
   private final String termTaxonomyTableName;

   private final String selectOptionSQL;
   private final String selectPostByIdSQL;

   /**
    * The configured site metadata.
    */
   final SiteMeta siteMeta;

   /**
    * The parent site for all posts and comments.
    */
   final ClientProtos.WireMessage.Site parentSite;

   /**
    * The parent source for all authors.
    */
   final ClientProtos.WireMessage.Source parentSource;
}
