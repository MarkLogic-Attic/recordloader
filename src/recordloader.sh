#!/bin/sh
#

BASE=`dirname $0`

CP=$BASE/../lib/recordloader.jar
CP=$CP:$HOME/lib/java/xcc.jar
CP=$CP:$HOME/lib/java/xpp3.jar

FILES=
VMARGS=

for a in $*; do
    if [ -e $a ]; then
        FILES="$FILES $a"
    else
        VMARGS="$VMARGS $a"
    fi
done

if [ -d "$JAVA_HOME" ]; then
  JAVA=$JAVA_HOME/bin/java
else
  JAVA=java
fi

$JAVA -cp $CP $VMARGS com.marklogic.ps.RecordLoader $FILES

# end recordloader.sh
