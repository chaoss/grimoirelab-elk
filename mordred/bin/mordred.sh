#!/bin/bash

function display_usage {
    echo "This script expects a configuration file ."
    echo -e "\nUsage:\n$0 [path_conf_file] \n"
}

function log {
    DATE=`date --rfc-3339=seconds`
    echo "$DATE $1" >> $MAINLOG 2>&1
}

function log_result {
    if [ $? -eq 0 ]
        then
            log "$1"
        else
            log "$2"
    fi
}

function set_variables {
    # It's replaced "-" or "." with "_" into the project name to avoid errors into MySQL
    PROJECT_SHORTNAME=`echo $PROJECT_SHORTNAME | sed "s/[-.]/_/g"`

    DB_SH=$PROJECT_SHORTNAME"_sh"
    DB_PRO=$PROJECT_SHORTNAME"_pro"

    SUPYBOT_ENRICHED_INDEX=$SUPYBOT_INDEX"_enriched"
    BUGZILLA_ENRICHED_INDEX=$BUGZILLA_INDEX"_enriched"
    JENKINS_ENRICHED_INDEX=$JENKINS_INDEX"_enriched"
    GERRIT_ENRICHED_INDEX=$GERRIT_INDEX"_enriched"
    GIT_ENRICHED_INDEX=$GIT_INDEX"_enriched"

    LOGS_DIR=$LOGS_DIR"/"$PROJECT_SHORTNAME
    MAINLOG=$LOGS_DIR"/main.log"
    if [ $DEBUG -eq 1 ]
        then
        echo "-- debugging variables --"
        echo "GIT_ENABLED=$GIT_ENABLED"
        echo "FROM_DATE=$FROM_DATE"
        echo "GERRIT_ENABLED=$GERRIT_ENABLED"
        echo "BUGZILLA_ENABLED=$BUGZILLA_ENABLED"
        echo "JENKINS_ENABLED=$JENKINS_ENABLED"
        echo "PROJECT_SHORTNAME=$PROJECT_SHORTNAME"
        echo "DB_HOST=$DB_HOST"
        echo "ORGS_ENABLED=$ORGS_ENABLED"
        echo "ES_URI=$ES_URI"
        echo "GERRIT_USER=$GERRIT_USER"
        echo "GERRIT_URL=$GERRIT_URL"
        echo "GITHUB_TOKEN=$GITHUB_TOKEN"
        echo "BUGZILLA_URL=$BUGZILLA_URL"
        echo "JENKINS_URL=$JENKINS_URL"
        echo "PROJECTS_JSON_FILE=$PROJECTS_JSON_FILE"
        echo "DB_SH="$DB_SH
        echo "DB_PRO="$DB_PRO
        echo "GIT_INDEX="$GIT_INDEX
        echo "GIT_ENRICHED_INDEX=$GIT_ENRICHED_INDEX"
        echo "GERRIT_INDEX="$GERRIT_INDEX
        echo "GERRIT_ENRICHED_INDEX=$GERRIT_ENRICHED_INDEX"
        echo "BUGZILLA_INDEX="$BUGZILLA_INDEX
        echo "JENKINS_INDEX=$JENKINS_INDEX"
        echo "JENKINS_ENRICHED_INDEX=$JENKINS_ENRICHED_INDEX"
        echo "SUPYBOT_INDEX=$SUPYBOT_INDEX"
        echo "SUPYBOT_ENRICHED_INDEX=$SUPYBOT_ENRICHED_INDEX"
        echo "SH_UNIFY_METHOD=$SH_UNIFY_METHOD"
        echo "SH_UNAFFILIATED_GROUP=$SH_UNAFFILIATED_GROUP"
        echo "LOGS_DIR=$LOGS_DIR"
        echo "MAINLOG=$MAINLOG"
        echo "--"
    fi
}

function create_dbs {
    echo "CREATE DATABASE IF NOT EXISTS $DB_SH ;"| mysql -uroot -h$DB_HOST
    echo "CREATE DATABASE IF NOT EXISTS $DB_PRO ;"| mysql -uroot -h$DB_HOST
}

function init_sh_orgs {
    if [ $ORGS_ENABLED -eq 1 ]
        then
        log "Organizations enabled"
        NORGS=`sortinghat -d $DB_SH --host $DB_HOST -u root orgs|wc -l`
        if [ $NORGS -gt 0 ]
        then
            log ". $NORGS organizations found in database (skipping loading)"
        else
            log ". No organizations found in database, loading .."
            if [ ! -z $ORGS_FILE ]
            then
                if [ -f $ORGS_FILE ]
                then
                    sortinghat -d $DB_SH --host $DB_HOST -u root load --orgs $ORGS_FILE >> $LOGS_DIR"/sortinghat.log" 2>&1
                    log_result ". Organizations loaded successfully" ". ERROR: something went wrong loading organizations"
                else
                    log ". ERROR: $ORGS_FILE does not exist!"
                    echo "ERROR: $ORGS_FILE does not exist!"
                    exit 1
                fi
            else
                log  ". ERROR: no variable ORG_FILE defined in configuration file"
            fi
        fi
        else
        log "Organizations disabled"
    fi
}


function compose_git_list {
    ## getting list of projects
    cd ~/VizGrimoireUtils/eclipse
    CACHED=${PROJECTS_JSON_FILE//\//_}
    rm -f $CACHED.json
    ./eclipse_projects.py -u $PROJECTS_JSON_FILE -s > $1
    log_result ". List of projects generated" ". ERROR: Error creating the Git list of projects"
}

function update_projects_db {
    cd ~/VizGrimoireUtils/eclipse
    CACHED=${PROJECTS_JSON_FILE//\//_}
    rm -f $CACHED.json
    TMP_FAKEAUTOMATOR=`mktemp`
    echo -e "[generic]\ndb_user = root\ndb_password=\ndb_projects=$DB_PRO\ndb_host=$DB_HOST\n[gerrit]\ntrackers=$GERRIT_URL" > $TMP_FAKEAUTOMATOR
    ## db projects
    ./eclipse_projects.py -u $PROJECTS_JSON_FILE -p -a $TMP_FAKEAUTOMATOR
    log_result "Projects database generated" "ERROR: Projects database not generated"
    rm $TMP_FAKEAUTOMATOR
}

function retrieve_data {
    ## git
    if [ $GIT_ENABLED -eq 1 ]
        then
        log "[git] Retrieving data"
        git_retrieval
        log "[git] Retrieval finished"
    fi

    if [ $GERRIT_ENABLED -eq 1 ]
        then
        ## gerrit
        log "[gerrit] Retrieving data"
        gerrit_retrieval
        log_result "[gerrit] Retrieval finished" "[gerrit] ERROR: Something went wrong with the retrieval"
    fi

    if [ $BUGZILLA_ENABLED -eq 1 ]
        then
        ## bugzilla
        log "[bugzilla] Retrieving data"
        bugzilla_retrieval
        log_result "[bugzilla] Retrieval finished" "[bugzilla] ERROR: Something went wrong with the retrieval"
    fi

    if [ $JENKINS_ENABLED -eq 1 ]
        then
        ## bugzilla
        log "[jenkins] Retrieving data"
        jenkins_retrieval
        log_result "[jenkins] Retrieval finished" "[jenkins] ERROR: Something went wrong with the retrieval"
    fi
    
    if [ $SUPYBOT_ENABLED -eq 1 ]
        then
        log "[supybot - irc] Retrieving data"
        supybot_retrieval
        log_result "[supybot - irc] Retrieval finished" "[supybot - irc] ERROR: Something went wrong with the retrieval"
    fi
    
}

function git_retrieval {
    TMP_GITLIST=`mktemp`
    compose_git_list $TMP_GITLIST
    cd ~/GrimoireELK/utils
    awk -v es_uri="$ES_URI" -v myindex="$GIT_INDEX" -v myfrom="$FROM_DATE" '{print "./p2o.py -e "es_uri" --index "myindex" -g git " $3" "myfrom}' $TMP_GITLIST | sh >> $LOGS_DIR"/git-collection.log" 2>&1
    rm $TMP_GITLIST
}

function gerrit_retrieval {
  cd ~/GrimoireELK/utils
  ./p2o.py -e $ES_URI -g --index $GERRIT_INDEX gerrit --user $GERRIT_USER --url $GERRIT_URL $GERRIT_EXTRA_PARAM $FROM_DATE >> $LOGS_DIR"/gerrit-collection.log" 2>&1
}

function bugzilla_retrieval {
  cd ~/GrimoireELK/utils
  ./p2o.py -e $ES_URI -g --index $BUGZILLA_INDEX bugzilla -u $BUGZILLA_USER -p $BUGZILLA_PASS $BUGZILLA_URL $FROM_DATE >> $LOGS_DIR"/bugzilla-collection.log" 2>&1
}

function jenkins_retrieval {
  cd ~/GrimoireELK/utils
  ./p2o.py -e $ES_URI -g --index $JENKINS_INDEX jenkins $JENKINS_URL $FROM_DATE >> $LOGS_DIR"/jenkins-collection.log" 2>&1
}

function supybot_retrieval {
  TMP_SUPYBOT_PROJECT_LIST=`mktemp`
  TMP_SUPYBOT_LIST=`mktemp`
  cd ~/GrimoireELK/utils
  ./projectinfo.py $PROJECTS_JSON_FILE list > $TMP_SUPYBOT_PROJECT_LIST
  IFS=$'\n'
  for project in $(cat $TMP_SUPYBOT_PROJECT_LIST);
  do
    ./projectinfo.py $PROJECTS_JSON_FILE list $project --repo irc list >> $TMP_SUPYBOT_LIST
  done
  for url in $(cat $TMP_SUPYBOT_LIST);
  do
    SUPYBOT_URL=`echo $url | cut -d \' -f 2`; SUPYBOT_PATH=`echo $url | cut -d \' -f 4`;
    ./p2o.py -e $ES_URI -g --index $SUPYBOT_INDEX supybot $SUPYBOT_URL $SUPYBOT_PATH $FROM_DATE >> $LOGS_DIR"/irc-collection.log" 2>&1
  done
  rm $TMP_SUPYBOT_PROJECT_LIST
  rm $TMP_SUPYBOT_LIST
}

function get_identities_from_data {
    ENR_EXTRA_FLAG='--only-identities'
    enrich_data $ENR_EXTRA_FLAG
}

function enrich_data_no_inc {
    ENR_EXTRA_FLAG='--no_incremental'
    enrich_data $ENR_EXTRA_FLAG
}

function enrich_data {
    if [ $GIT_ENABLED -eq 1 ]
        then
        log "[git] Git p2o starts"
        git_enrichment $1
        log_result "[git] p2o finished" "[git] ERROR: Something went wrong with p2o"
    fi

    if [ $GERRIT_ENABLED -eq 1 ]
        then
        log "[gerrit] Gerrit p2o starts"
        gerrit_enrichment $1
        log_result "[gerrit] p2o finished" "[gerrit] ERROR: Something went wrong with p2o"
    fi

    if [ $BUGZILLA_ENABLED -eq 1 ]
        then
        log "[bugzilla] Bugzilla p2o starts"
        bugzilla_enrichment $1
        log_result "[bugzilla] p2o finished" "[bugzilla] ERROR: Something went wrong with p2o"
    fi

    if [ $JENKINS_ENABLED -eq 1 ]
        then
        log "[jenkins] Jenkins p2o starts"
        jenkins_enrichment $1
        log_result "[jenkins] p2o finished" "[jenkins] ERROR: Something went wrong with p2o"
    fi

    if [ $SUPYBOT_ENABLED -eq 1 ]
        then
        log "[supybot - irc] Supybot p2o starts"
        supybot_enrichment $1
        log_result "[supybot - irc] p2o finished" "[supybot - irc] ERROR: Something went wrong with p2o"
    fi

}

function git_enrichment {
    ENR_EXTRA_FLAG=$1
    GITHUB_PARAMETER=''
    if [ ! -z $GITHUB_TOKEN ]
        then
        GITHUB_PARAMETER="--github-token $GITHUB_TOKEN"
    fi
    cd ~/GrimoireELK/utils
    ./p2o.py --db-sortinghat $DB_SH --db-projects-map $DB_PRO -e $ES_URI -g --studies --enrich_only $ENR_EXTRA_FLAG $GITHUB_PARAMETER --index $GIT_INDEX --index-enrich $GIT_ENRICHED_INDEX git '' >> $LOGS_DIR"/git-enrichment.log" 2>&1
}

function gerrit_enrichment {
    ENR_EXTRA_FLAG=$1
    cd ~/GrimoireELK/utils
    ./p2o.py --db-sortinghat $DB_SH --db-projects-map $DB_PRO -e $ES_URI -g --enrich_only $ENR_EXTRA_FLAG --index $GERRIT_INDEX --index-enrich $GERRIT_ENRICHED_INDEX gerrit --user $GERRIT_USER --url $GERRIT_URL >> $LOGS_DIR"/gerrit-enrichment.log" 2>&1
}

function bugzilla_enrichment {
    ENR_EXTRA_FLAG=$1
    cd ~/GrimoireELK/utils
    ./p2o.py --db-sortinghat $DB_SH --db-projects-map $DB_PRO -e $ES_URI -g --enrich_only $ENR_EXTRA_FLAG --index $BUGZILLA_INDEX --index-enrich $BUGZILLA_ENRICHED_INDEX bugzilla $BUGZILLA_URL >> $LOGS_DIR"/bugzilla-enrichment.log" 2>&1
}

function jenkins_enrichment {
    ENR_EXTRA_FLAG=$1
    cd ~/GrimoireELK/utils
    ./p2o.py --db-sortinghat $DB_SH --db-projects-map $DB_PRO -e $ES_URI -g --enrich_only $ENR_EXTRA_FLAG --index $JENKINS_INDEX --index-enrich $JENKINS_ENRICHED_INDEX jenkins $JENKINS_URL >> $LOGS_DIR"/jenkins-enrichment.log" 2>&1
}

function supybot_enrichment {
    TMP_SUPYBOT_PROJECT_LIST=`mktemp`
    TMP_SUPYBOT_LIST=`mktemp`
    ENR_EXTRA_FLAG=$1
    cd ~/GrimoireELK/utils
    ./projectinfo.py $PROJECTS_JSON_FILE list > $TMP_SUPYBOT_PROJECT_LIST
    IFS=$'\n'
    for project in $(cat $TMP_SUPYBOT_PROJECT_LIST);
    do
      ./projectinfo.py $PROJECTS_JSON_FILE list $project --repo irc list >> $TMP_SUPYBOT_LIST
    done
    for url in $(cat $TMP_SUPYBOT_LIST);
    do
      SUPYBOT_URL=`echo $url | cut -d \' -f 2`; SUPYBOT_PATH=`echo $url | cut -d \' -f 4`;
      ./p2o.py --db-sortinghat $DB_SH --db-projects-map $DB_PRO -e $ES_URI -g --enrich_only $ENR_EXTRA_FLAG --index $SUPYBOT_INDEX --index-enrich $SUPYBOT_ENRICHED_INDEX supybot $SUPYBOT_URL $SUPYBOT_PATH >> $LOGS_DIR"/irc-enrichment.log" 2>&1
    done
    rm $TMP_SUPYBOT_PROJECT_LIST
    rm $TMP_SUPYBOT_LIST
}

function sortinghat_unify {
    if [ -z $SH_UNIFY_METHOD ]
        then
        SH_UNIFY_METHOD="email-name"
    fi
    log "[sortinghat] Unify using $SH_UNIFY_METHOD algorithm"
    sortinghat -u root -p '' --host $DB_HOST -d $DB_SH unify -m $SH_UNIFY_METHOD --fast-matching >> $LOGS_DIR"/sortinghat.log" 2>&1
}

function sortinghat_affiliate {
    sortinghat -u root -p '' --host $DB_HOST -d $DB_SH affiliate >> $LOGS_DIR"/sortinghat.log" 2>&1
}

function group_unaffiliated_people {
    if [ ! -z $SH_UNAFFILIATED_GROUP ]
        then
        log "[sortinghat] Affiliating the unaffiliated people to group $SH_UNAFFILIATED_GROUP"
        sortinghat --host $DB_HOST -d $DB_SH orgs -a $SH_UNAFFILIATED_GROUP >> $LOGS_DIR"/sortinghat.log" 2>&1
        mysql -uroot -h$DB_HOST $DB_SH -e "INSERT INTO enrollments (start, end, uuid, organization_id) SELECT '1900-01-01 00:00:00','2100-01-01 00:00:00', A.uuid,B.id FROM (select DISTINCT uuid from uidentities where uuid NOT IN (SELECT DISTINCT uuid from enrollments)) A, (SELECT id FROM organizations WHERE name = '$SH_UNAFFILIATED_GROUP') B;"
        log_result "[sortinghat] Affiliation for $SH_UNAFFILIATED_GROUP finished" "[sortinghat] ERROR: Something went wrong with the affiliation of $SH_UNAFFILIATED_GROUP"
    else
        log "[sortinghat] (WARNING!) No variable defined SH_UNAFFILIATED_GROUP"
    fi
}

function go_to_bed {
  if [ ! -z $SLEEP_TIME ]
      then
      log "Waiting $SLEEP_TIME secdons until next update starts"
      sleep $SLEEP_TIME
  else
      log "No SLEEP_TIME variable defined, so starting next update now"
  fi
}

####
# main
####

if [  $# -le 0 ]
    then
    display_usage
    exit 1
fi

CONF_FILE=$1
source $CONF_FILE
set_variables

mkdir -p $LOGS_DIR

log "****"
log "loop starts"
log "Creating databases for SH and projects (if dont exist)"
create_dbs

init_sh_orgs


while true; do

    SECONDS=0

    log "---"
    log "Dashboard update starting"

    if [ ! -z $ADHOC_SCRIPT ]
        then
        $ADHOC_SCRIPT $CONF_FILE >> $MAINLOG 2>&1
        log_result "Ad hoc script executed successfully ($ADHOC_SCRIPT)" "ERROR: Ad hoc script finished with errors ($ADHOC_SCRIPT)"
    fi

    log "Building projects database"
    update_projects_db

    if [ -z $SKIP_COLLECTION ] || [ $SKIP_COLLECTION -eq 0 ]
        then

        START_TIME=$SECONDS
        retrieve_data
        ELAPSED_TIME=$(($SECONDS - $START_TIME))
        log "(T) Data collected in $(($ELAPSED_TIME / 60)) minutes and $(($ELAPSED_TIME % 60)) seconds."
    fi

    if [ -z $SKIP_SH ] || [ $SKIP_SH -eq 0 ]
        then

        log "Getting identities from raw data and executing sortinghat"
        START_TIME=$SECONDS
        get_identities_from_data

        log "[sortinghat] Unify starts"
        sortinghat_unify
        log_result "[sortinghat] Unify finished" "[sortinghat] ERROR: Something went wrong with the unify"

        log "[sortinghat] Affiliate starts"
        sortinghat_affiliate
        log_result "[sortinghat] Affiliate finished" "[sortinghat] ERROR: Something went wrong with the affiliate"

        group_unaffiliated_people

        ELAPSED_TIME=$(($SECONDS - $START_TIME))
        log "(T) Identities and sortinghat done in $(($ELAPSED_TIME / 60)) minutes and $(($ELAPSED_TIME % 60)) seconds."
    fi

    if [ -z $SKIP_ENRICHMENT ] || [ $SKIP_ENRICHMENT -eq 0 ]
        then

        log "Enrichment starts"
        START_TIME=$SECONDS

        enrich_data

        ELAPSED_TIME=$(($SECONDS - $START_TIME))
        log "(T) Data enriched in $(($ELAPSED_TIME / 60)) minutes and $(($ELAPSED_TIME % 60)) seconds."
    fi

    duration=$SECONDS
    log "(T) Dashboard updated in $(($duration / 60)) minutes and $(($duration % 60)) seconds."

    if [ -z $DEBUG ] || [ $DEBUG -eq 0 ]
        then
        go_to_bed
    else
        log "Debug mode enabled. Loop disabled, exiting now"
        exit 0
    fi

done
