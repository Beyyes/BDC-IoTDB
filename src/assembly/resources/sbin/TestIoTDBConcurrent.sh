
# this function is for parsing the variables like "A=B" in  `start-server.sh -D A=B`
# The command just parse IOTDB-prefixed variables and ignore all other variables
checkEnvVariables()
{
  string="$1"
  array=$(echo $string | tr '=' ' ')
  eval set -- "$array"
  case "$1" in
          IOTDB_INCLUDE)
               IOTDB_INCLUDE="$2"
          ;;
          IOTDB_CLI_CONF)
              IOTDB_CLI_CONF="$2"
          ;;
          *)
            #do nothing
          ;;
      esac
}

PARAMETERS=""

# Added parameters when default parameters are missing
user_param="-u root"
passwd_param="-pw root"
host_param="-h 127.0.0.1"
port_param="-p 6667"

while true; do
    case "$1" in
        -u)
            user_param="-u $2"
            shift 2
            ;;
        -pw)
            passwd_param="-pw $2"
            shift 2
        ;;
        -h)
            host_param="-h $2"
            shift 2
        ;;
        -p)
            port_param="-p $2"
            shift 2
        ;;
        -D)
            checkEnvVariables $2
            shift 2
        ;;
        --help)
            echo "Usage: $0 [-h <ip>] [-p <port>] [-u <username>] [-pw <password>] [-D <name=value>] [-c] [-e sql] [-maxPRC <PRC size>]"
            exit 0
        ;;
        "")
              #if we do not use getopt, we then have to process the case that there is no argument.
              #in some systems, when there is no argument, shift command may throw error, so we skip directly
              break
              ;;
        *)
            PARAMETERS="$PARAMETERS $1"
            shift
        ;;
    esac
done


if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

IOTDB_CLI_CONF=${IOTDB_HOME}/conf

MAIN_CLASS=org.apache.iotdb.TestIoTDBConcurrent

for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

set -o noglob
iotdb_cli_params="-Dlogback.configurationFile=${IOTDB_CLI_CONF}/logback-cli.xml"
# echo ""$JAVA" $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" 172.20.31.26 1000"
exec "$JAVA" $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" 10

exit $?
