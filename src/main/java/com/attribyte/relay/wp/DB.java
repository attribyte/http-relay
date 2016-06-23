
package com.attribyte.relay.wp;

import com.attribyte.client.ClientProtos;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.attribyte.sql.pool.ConnectionPool;
import org.attribyte.util.SQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class DB {

   DB(final ConnectionPool connectionPool,
      final String namespace,
      final Set<String> allowedStatus,
      final Set<String> allowedTypes,
      final long siteId) throws SQLException {
      this.connectionPool = connectionPool;
      this.namespace = namespace;
      this.allowedStatus = allowedStatus == null ? DEFAULT_ALLOWED_STATUS : ImmutableSet.copyOf(allowedStatus);
      this.allowedTypes = allowedTypes == null ? DEFAULT_ALLOWED_TYPES : ImmutableSet.copyOf(allowedTypes);

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
      this.siteMeta = selectSiteMeta(siteId);
   }

   private static final String selectPostSQL = "SELECT ID, post_author, post_date_gmt, " +
           "post_title, post_excerpt, post_content, post_status, post_modified_gmt, post_type FROM ";

   /**
    * Builds a post from a result set.
    * @param rs The result set.
    * @param allowedStatus The set of allowed values for status (lower-case).
    * @param allowedTypes The set of allowed post types (lower-case).
    * @return The post, or {@code absent} if post was not found or not allowed.
    * @throws SQLException on database error.
    */
   private Optional<ClientProtos.WireMessage.Entry.Builder> postFromResultSet(final ResultSet rs,
                                                                              final Set<String> allowedStatus,
                                                                              final Set<String> allowedTypes) throws SQLException {

      if(!allowedStatus.contains(Strings.nullToEmpty(rs.getString(7)).trim().toLowerCase())) {
         return Optional.absent();
      }

      if(!allowedTypes.contains(Strings.nullToEmpty(rs.getString(9)).trim().toLowerCase())) {
         return Optional.absent();
      }

      return Optional.of(ClientProtos.WireMessage.Entry.newBuilder()
              .setUID(buildId(rs.getLong(1)))
              .setAuthor(ClientProtos.WireMessage.Author.newBuilder().setId(rs.getLong(2)))
              .setPublishTimeMillis(rs.getTimestamp(3).getTime())
              .setTitle(rs.getString(4))
              .setSummary(rs.getString(5))
              .setContent(rs.getString(6))
              .setLastModifiedMillis(rs.getTimestamp(8).getTime()));
   }

   /*
      In our experience, ahem, it is not uncommon for publishing systems to have long sequences
      of posts with identical modification times (because of bulk-updates, etc.)
      We'll deal with that by requiring both the modified time to select after
      and the last id we've seen.
    */

   private static final String selectModPostsPrefixSQL = "SELECT ID, post_modified_gmt, post_status, post_type FROM ";
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
         stmt.setTimestamp(0, new Timestamp(start.lastModifiedMillis));
         stmt.setTimestamp(1, new Timestamp(start.lastModifiedMillis));
         stmt.setLong(2, start.id);
         stmt.setInt(3, limit);
         rs = stmt.executeQuery();
         while(rs.next()) {
            metaList.add(new PostMeta(rs.getLong(1), rs.getTimestamp(2).getTime(), rs.getString(3), rs.getString(4)));
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
    * Builds an id with optional namespace.
    * @param id The numeric id.
    * @return The id.
    */
   private ClientProtos.WireMessage.Id buildId(final long id) {
      return namespace == null ? ClientProtos.WireMessage.Id.newBuilder().setId(Long.toString(id)).build() :
              ClientProtos.WireMessage.Id.newBuilder().setId(Long.toString(id)).setNamespace(namespace).build();
   }


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
         return rs.next() ? Optional.of(rs.getString(1)) : Optional.absent();
      } finally {
         SQLUtil.closeQuietly(conn, stmt, rs);
      }
   }

   private final ConnectionPool connectionPool;
   private final String namespace;
   private final LoadingCache<Long, Optional<UserMeta>> userCache;
   private final LoadingCache<Long, Optional<TermMeta>> termCache;


   private final String postsTableName;
   private final String commentsTableName;
   private final String optionsTableName;
   private final String termsTableName;
   private final String termRelationshipsTableName;
   private final String termTaxonomyTableName;
   private final Set<String> allowedStatus;
   private final Set<String> allowedTypes;
   private final String selectOptionSQL;
   private final SiteMeta siteMeta;

   /**
    * The default set of allowed post status ('publish' only).
    * @see <a href="https://codex.wordpress.org/Post_Status">https://codex.wordpress.org/Post_Status</a>
    */
   public static final ImmutableSet<String> DEFAULT_ALLOWED_STATUS = ImmutableSet.of("publish");

   /**
    * The default set of allowed post type ('post' only).
    * @see <a href="https://codex.wordpress.org/Post_Types">https://codex.wordpress.org/Post_Types</a>
    */
   public static final ImmutableSet<String> DEFAULT_ALLOWED_TYPES = ImmutableSet.of("post");
}
