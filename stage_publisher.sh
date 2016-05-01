#!/bin/sh
VERSION="0.5.0"
cp attribyte-async-publisher-0.5.pom dist_publisher/lib/attribyte-async-publisher-${VERSION}.pom
cd dist_publisher/lib
gpg -ab attribyte-async-publisher-${VERSION}.pom
gpg -ab attribyte-async-publisher-${VERSION}.jar
gpg -ab attribyte-async-publisher-${VERSION}-sources.jar
gpg -ab attribyte-async-publisher-${VERSION}-javadoc.jar
jar -cvf ../bundle.jar *

