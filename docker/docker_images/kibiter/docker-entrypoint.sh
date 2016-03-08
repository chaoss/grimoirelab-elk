#!/bin/bash

set -e

# Add kibana as command if needed
if [[ "$1" == -* ]]; then
	set -- kibana "$@"
fi

# Run as user "kibana" if the command is "kibana"
if [ "$1" = 'kibana' ]; then
	if [ "$ELASTICSEARCH_URL" -o "$ELASTICSEARCH_PORT_9200_TCP" ]; then
		: ${ELASTICSEARCH_URL:='http://elasticsearch:9200'}
		sed -ri "s!^(\#\s*)?(elasticsearch\.url:).*!\2 '$ELASTICSEARCH_URL'!" /opt/kibana/config/kibana.yml
	else
		echo >&2 'warning: missing ELASTICSEARCH_PORT_9200_TCP or ELASTICSEARCH_URL'
		echo >&2 '  Did you forget to --link some-elasticsearch:elasticsearch'
		echo >&2 '  or -e ELASTICSEARCH_URL=http://some-elasticsearch:9200 ?'
		echo >&2
	fi

        if [ "$BASE_PATH" != "" ]; then
                sed -e "s|^# server.basePath: \"\".*$|server.basePath: \"$BASE_PATH\"|" -i /opt/kibana/config/kibana.yml
                sed -e "s|^# server.basePath: \"\".*$|server.basePath: \"$BASE_PATH\"|" -i /opt/kibana/src/config/kibana.yml
        fi

        if [ "$PROJECT_NAME" != "" ]; then
                sed -e "s/'title': ''$/'title': '$PROJECT_NAME'/" -i /opt/kibana/src/plugins/kibana/public/kibana.js
                sed -e "s/'title': ''$/'title': '$PROJECT_NAME'/" -i /opt/kibana/optimize/bundles/kibana.bundle.js
        fi
	
	set -- gosu kibana "$@"
fi

exec "$@"
