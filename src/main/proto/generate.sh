SRC_DIR="./"
DST_DIR="../java"
protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/Client.proto