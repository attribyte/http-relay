#!/bin/sh
VERSION="0.5.0"
cp attribyte-http-relay-0.5.pom dist/lib/attribyte-http-relay-${VERSION}.pom
cd dist/lib
gpg -ab attribyte-http-relay-${VERSION}.pom
gpg -ab attribyte-http-relay-${VERSION}.jar
gpg -ab attribyte-http-relay-${VERSION}-sources.jar
gpg -ab attribyte-http-relay-${VERSION}-javadoc.jar
jar -cvf ../bundle.jar *

