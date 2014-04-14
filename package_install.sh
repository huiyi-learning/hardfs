# remove previous build
rm -rf hadoop-dist/target/hadoop-2.2.0

# package
mvn package -Pdist,native -DskipTests -Dtar

# uncompress
tar xvf hadoop-dist/target/hadoop-2.2.0.tar.gz

# change conf
