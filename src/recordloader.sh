#!/bin/sh
#

CP=../lib/recordloader.jar
CP=$CP:$HOME/lib/java/xcc.jar
CP=$CP:$HOME/lib/java/xpp3.jar

$JAVA_HOME/bin/java -cp $CP com.marklogic.ps.RecordLoader $*

# end recordloader.sh
