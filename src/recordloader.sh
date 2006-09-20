#!/bin/sh
#

CP=../lib/recordloader.jar
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

$JAVA_HOME/bin/java -cp $CP $VMARGS com.marklogic.ps.RecordLoader $FILES

# end recordloader.sh
