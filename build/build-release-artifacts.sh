#!/bin/bash

mvn assembly:assembly

export ARCHIVE=apache-vxquery-0.2-incubating-src.zip
gpg2 --armor --output ${ARCHIVE}.asc --detach-sig target/${ARCHIVE}
cat target/${ARCHIVE} | openssl dgst -sha1 | sed -e's/(stdin)= //' > ${ARCHIVE}.sha1
cat target/${ARCHIVE} | openssl dgst -md5 | sed -e's/(stdin)= //' > ${ARCHIVE}.md5

#mvn site site:deploy
