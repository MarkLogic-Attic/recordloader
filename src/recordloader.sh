#!/bin/sh
#

CP=$HOME/lib/java/recordloader.jar
CP=$CP:$HOME/lib/java/xdbc.jar
CP=$CP:$HOME/lib/java/xdmp.jar
CP=$CP:$HOME/lib/java/xpp3.jar

$JAVA_HOME/bin/java -Dfile.encoding=UTF-8 -cp $CP \
  com.marklogic.ps.RecordLoader $*

# end recordloader.sh
