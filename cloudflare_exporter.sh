#!/usr/bin/env bash

set -Eeo pipefail

dependencies=(awk cat curl date gzip jq)
for program in "${dependencies[@]}"; do
    command -v "$program" >/dev/null 2>&1 || {
        echo >&2 "Couldn't find dependency: $program. Aborting."
        exit 1
    }
done

AWK=$(command -v awk)
CAT=$(command -v cat)
CURL=$(command -v curl)
DATE=$(command -v date)
GZIP=$(command -v gzip)
JQ=$(command -v jq)

if [[ "${RUNNING_IN_DOCKER}" ]]; then
    source "/app/cloudflare_exporter.conf"
    CLOUDFLARE_ZONE_LIST=$($CAT /app/cloudflare_zone_list.json)
    CLOUDFLARE_KV_NAMESPACES=$($CAT /app/cloudflare_kv_namespaces_list.conf)
elif [[ -f $CREDENTIALS_DIRECTORY/creds ]]; then
    #shellcheck source=/dev/null
    source "$CREDENTIALS_DIRECTORY/creds"
    CLOUDFLARE_ZONE_LIST=$($CAT $CREDENTIALS_DIRECTORY/list)
    CLOUDFLARE_KV_NAMESPACES=$($CAT $CREDENTIALS_DIRECTORY/namespaces_list)
else
    source "./cloudflare_exporter.conf"
    CLOUDFLARE_ZONE_LIST=$($CAT ./cloudflare_zone_list.json)
    CLOUDFLARE_KV_NAMESPACES=$($CAT ./cloudflare_kv_namespaces_list.conf)
fi

[[ -z "${INFLUXDB_HOST}" ]] && echo >&2 "INFLUXDB_HOST is empty. Aborting" && exit 1
[[ -z "${INFLUXDB_API_TOKEN}" ]] && echo >&2 "INFLUXDB_API_TOKEN is empty. Aborting" && exit 1
[[ -z "${ORG}" ]] && echo >&2 "ORG is empty. Aborting" && exit 1
[[ -z "${BUCKET}" ]] && echo >&2 "BUCKET is empty. Aborting" && exit 1
[[ -z "${CLOUDFLARE_API_TOKEN}" ]] && echo >&2 "CLOUDFLARE_API_TOKEN is empty. Aborting" && exit 1
[[ -z "${CLOUDFLARE_ZONE_LIST}" ]] && echo >&2 "CLOUDFLARE_ZONE_LIST is empty. Aborting" && exit 1
[[ -z "${CLOUDFLARE_ACCOUNT_TAG}" ]] && echo >&2 "CLOUDFLARE_ACCOUNT_TAG is empty. Aborting" && exit 1
[[ $(echo "${CLOUDFLARE_ZONE_LIST}" | $JQ type 1>/dev/null) ]] && echo >&2 "CLOUDFLARE_ZONE_LIST is not valid JSON. Aborting" && exit 1
[[ -n "${CLOUDFLARE_ACCOUNT_EMAIL}" ]] && CF_EMAIL_HEADER="X-Auth-Email: ${CLOUDFLARE_ACCOUNT_EMAIL}"


RFC_CURRENT_DATE=$($DATE --rfc-3339=date)
#CURRENT_UNIXTS=$($DATE +%s -d ISO_CURRENT_DATE_TIME )
CURRENT_UNIXTS=$($DATE +%s )
ISO_CURRENT_DATE_TIME=$($DATE --iso-8601=seconds)
ISO_CURRENT_DATE_TIME_5M_AGO=$($DATE --iso-8601=seconds --date "5 minute ago")
ISO_CURRENT_DATE_TIME_1H_AGO=$($DATE --iso-8601=seconds --date "1 hour ago")
ISO_CURRENT_DATE_TIME_2H_AGO=$($DATE --iso-8601=seconds --date "2 hour ago")
ISO_CURRENT_DATE_TIME_1D_AGO=$($DATE --iso-8601=seconds --date "24 hour ago")

[[ -z "$TIMESPAN" ]] && TIMESPAN=5M
REFERENCE_DATE="$ISO_CURRENT_DATE_TIME_1H_AGO"
[[ "$TIMESPAN" = "5M" ]]  && REFERENCE_DATE="$ISO_CURRENT_DATE_TIME_5M_AGO"
[[ "$TIMESPAN" = "1H" ]]  && REFERENCE_DATE="$ISO_CURRENT_DATE_TIME_1H_AGO"
[[ "$TIMESPAN" = "1D" ]]  && REFERENCE_DATE="$ISO_CURRENT_DATE_TIME_1D_AGO"

[[ -z "$INFLUXDB_URL" ]] && INFLUXDB_URL="https://$INFLUXDB_HOST/api/v2/write?precision=ns&org=$ORG&bucket=$BUCKET"

TMPDATABASE=/tmp/influx.${CLOUDFLARE_ACCOUNT_TAG}.stats
#enable bearer auth for grafana
echo "$INFLUXDB_API_TOKEN"|grep -q "Token "  && INFLUXAUTHSTRING="$INFLUXDB_API_TOKEN"
echo "$INFLUXDB_API_TOKEN"|grep -q "Bearer " && INFLUXAUTHSTRING="$INFLUXDB_API_TOKEN"
[[ -z "$INFLUXAUTHSTRING" ]] && INFLUXAUTHSTRING="Token $INFLUXDB_API_TOKEN"

CF_URL="https://api.cloudflare.com/client/v4/graphql"

nb_zones=$(echo "$CLOUDFLARE_ZONE_LIST" | $JQ 'length - 1')

for i in $(seq 0 "$nb_zones"); do

    mapfile -t cf_zone < <(echo "$CLOUDFLARE_ZONE_LIST" | $JQ --raw-output ".[${i}] | .id, .domain")
    cf_zone_id=${cf_zone[0]}
    cf_zone_domain="${cf_zone[1]}"

    GRAPHQL_QUERY=$(
        cat <<END_HEREDOC
{ "query":
  "query {
    viewer {
      zones(filter: {zoneTag: \$zoneTag}) {
        httpRequests1hGroups(limit:7, filter: \$filter,)   {
          dimensions {
            datetime
          }
          sum {
            browserMap {
              pageViews
              uaBrowserFamily
            }
            bytes
            cachedBytes
            cachedRequests
            contentTypeMap {
              bytes
              requests
              edgeResponseContentTypeName
            }
            countryMap {
              bytes
              requests
              threats
              clientCountryName
            }
            encryptedBytes
            encryptedRequests
            ipClassMap {
              requests
              ipType
            }
            pageViews
            requests
            responseStatusMap {
              requests
              edgeResponseStatus
            }
            threats
            threatPathingMap {
              requests
              threatPathingName
            }
          }
          uniq {
            uniques
          }
        }
      }
    }
  }",
  "variables": {
    "zoneTag": "$cf_zone_id",
    "filter": {
      "date_geq": "$REFERENCE_DATE",
      "date_leq": "$RFC_CURRENT_DATE"
    }
  }
}
END_HEREDOC
    )

    cf_json=$(
        $CURL --silent --fail --show-error --compressed \
            --request POST \
            --header "Content-Type: application/json" \
            --header "$CF_EMAIL_HEADER" \
            --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
            --data "$(echo -n $GRAPHQL_QUERY)" \
            "$CF_URL"
    )

    cf_nb_errors=$(echo $cf_json | $JQ ".errors | length")

    if [[ $cf_nb_errors -gt 0 ]]; then
        cf_errors=$(echo $cf_json | $JQ --raw-output ".errors[] | .message")
        printf "Cloudflare API request failed with: \n%s\nAborting\n" "$cf_errors" >&2
        exit 1
    fi

    cf_nb_groups=$(echo $cf_json | $JQ ".data.viewer.zones[0].httpRequests1hGroups | length - 1")

    if [[ $cf_nb_groups -gt 0 ]]; then

        for i in $(seq 0 "$cf_nb_groups"); do
            cf_json_parsed=$(echo $cf_json | $JQ ".data.viewer.zones[0].httpRequests1hGroups[$i]")
            date_value=$(echo $cf_json_parsed | $JQ --raw-output '.dimensions.datetime')
            uniques=$(echo $cf_json_parsed | $JQ '.uniq.uniques // 0')
            ts=$($DATE "+%s" --date="$date_value")

            mapfile -t cf_root_values < <(
                echo $cf_json_parsed | $JQ \
                    '.sum | .bytes // 0, .cachedBytes // 0, .cachedRequests // 0, .encryptedBytes, .encryptedRequests // 0, .pageViews // 0, .requests // 0, .threats // 0'
            )

            nb_browsers=$(echo $cf_json_parsed | $JQ '.sum.browserMap | length - 1')
            nb_content_types=$(echo $cf_json_parsed | $JQ '.sum.contentTypeMap | length - 1')
            nb_countries=$(echo $cf_json_parsed | $JQ '.sum.countryMap | length - 1')
            nb_ip_classes=$(echo $cf_json_parsed | $JQ '.sum.ipClassMap | length - 1')
            nb_response_status=$(echo $cf_json_parsed | $JQ '.sum.responseStatusMap | length - 1')
            nb_threat_pathing=$(echo $cf_json_parsed | $JQ '.sum.threatPathingMap | length - 1')

            if [[ $nb_browsers -gt 0 ]]; then
                for j in $(seq 0 "$nb_browsers"); do
                    mapfile -t cf_browser_values < <(
                        echo $cf_json_parsed | $JQ --raw-output ".sum.browserMap[$j] | .uaBrowserFamily, .pageViews // 0"
                    )
                    cf_stats+=$(
                        printf "\ncloudflare_stats_browser,zone=%s,browserFamily=%s pageViews=%s %s" \
                            "$cf_zone_domain" "${cf_browser_values[0]}" "${cf_browser_values[1]}" "$ts"
                    )
                done
            fi

            if [[ $nb_content_types -gt 0 ]]; then
                for k in $(seq 0 "$nb_content_types"); do
                    mapfile -t cf_ct_values < <(
                        echo $cf_json_parsed | $JQ --raw-output ".sum.contentTypeMap[$k] | .bytes // 0, .edgeResponseContentTypeName, .requests // 0"
                    )
                    cf_stats+=$(
                        printf "\ncloudflare_stats_content_type,zone=%s,edgeResponse=%s bytes=%s,requests=%s %s" \
                            "$cf_zone_domain" "${cf_ct_values[1]}" "${cf_ct_values[0]}" "${cf_ct_values[2]}" "$ts"
                    )
                done
            fi

            if [[ $nb_countries -gt 0 ]]; then
                for l in $(seq 0 "$nb_countries"); do
                    mapfile -t cf_country_values < <(
                        echo $cf_json_parsed | $JQ --raw-output ".sum.countryMap[$l] | .clientCountryName, .bytes // 0, .requests // 0, .threats // 0"
                    )
                    cf_stats+=$(
                        printf \
                            "\ncloudflare_stats_countries,zone=%s,country=%s bytes=%s,requests=%s,threats=%s %s" \
                            "$cf_zone_domain" "${cf_country_values[0]}" "${cf_country_values[1]}" \
                            "${cf_country_values[2]}" "${cf_country_values[3]}" \
                            "$ts"
                    )
                done
            fi

            if [[ $nb_ip_classes -gt 0 ]]; then
                for m in $(seq 0 "$nb_ip_classes"); do
                    mapfile -t cf_ip_values --raw-output < <(echo $cf_json_parsed | $JQ ".sum.ipClassMap[$m] | .ipType, .requests // 0")
                    cf_stats+=$(
                        printf \
                            "\ncloudflare_stats_ip,zone=%s,ipType=%s requests=%s %s" \
                            "$cf_zone_domain" "${cf_ip_values[0]}" "${cf_ip_values[1]}" "$ts"
                    )
                done
            fi

            if [[ $nb_response_status -gt 0 ]]; then
                for n in $(seq 0 "$nb_response_status"); do
                    mapfile -t cf_response_values < <(
                        echo $cf_json_parsed | $JQ ".sum.responseStatusMap[$n] | .edgeResponseStatus, .requests // 0"
                    )
                    cf_stats+=$(
                        printf \
                            "\ncloudflare_stats_responses,zone=%s,status=%s requests=%s %s" \
                            "$cf_zone_domain" "${cf_response_values[0]}" "${cf_response_values[1]}" "$ts"
                    )
                done
            fi

            if [[ $nb_threat_pathing -gt 0 ]]; then
                for o in $(seq 0 "$nb_response_status"); do
                    mapfile -t cf_threat_values < <(
                        echo $cf_json_parsed | $JQ --raw-output ".sum.threatPathingMap[$o] | .threatPathingMap, .requests // 0"
                    )
                    cf_stats+=$(
                        printf \
                            "\ncloudflare_stats_threats,zone=%s,threat=%s requests=%s %s" \
                            "$cf_zone_domain" "${cf_threat_values[0]}" "${cf_threat_values[1]}" "$ts"
                    )
                done
            fi

            cf_stats+=$(
                printf \
                    "\ncloudflare_stats,zone=%s bytes=%s,cachedBytes=%s,cachedRequests=%s,encryptedBytes=%s,encryptedRequests=%s,pageViews=%s,requests=%s,threats=%s,uniqueVisitors=%s %s" \
                    "$cf_zone_domain" \
                    "${cf_root_values[0]}" "${cf_root_values[1]}" "${cf_root_values[2]}" "${cf_root_values[3]}" \
                    "${cf_root_values[4]}" "${cf_root_values[5]}" "${cf_root_values[6]}" "${cf_root_values[7]}" \
                    "$uniques" \
                    "$ts"
            )
        done
        echo -n  "$cf_stats" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep -q ^0$ && ( echo "empty cf_stats")
        echo -n  "$cf_stats" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep -q ^0$ || ( echo "$cf_stats" >> "${TMPDATABASE}" )
        
        #echo -n  "$cf_stats" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( 
        #    echo "$cf_stats" | sed 's/^\t\+//g;s/^ \+//g' | sed "s~$~000000000~g"| $GZIP |
        #        $CURL --silent --fail --show-error \
        #            --request POST "${INFLUXDB_URL}" \
        #            --header 'Content-Encoding: gzip' \
        #            --header "Authorization: $INFLUXAUTHSTRING" \
        #            --header "Content-Type: text/plain; charset=utf-8" \
        #            --header "Accept: application/json" \
        #             --data-binary @- )
    fi
done

WORKERS_GRAPHQL_QUERY=$(
    cat <<END_HEREDOC
{ "query":
  "query GetWorkersAnalytics(\$accountTag: string, \$datetimeStart: string, \$datetimeEnd: string) {
    viewer {
      accounts(filter: {accountTag: \$accountTag}) {
        workersInvocationsAdaptive(limit: 100, filter: {
          datetime_geq: \$datetimeStart,
          datetime_leq: \$datetimeEnd
        }) {
          sum {
            clientDisconnects
            cpuTimeUs
            duration
            errors
            requests
            subrequests
            responseBodySize
            wallTime
          }
          quantiles {
            cpuTimeP50
            cpuTimeP99
            durationP50
            durationP99
            responseBodySizeP50
            responseBodySizeP99
            wallTimeP50
            wallTimeP99
          }
          dimensions{
            scriptName
            status
          }
        }
      }
    }
  }",
  "variables": {
    "accountTag": "$CLOUDFLARE_ACCOUNT_TAG",
    "datetimeStart": "$REFERENCE_DATE",
    "datetimeEnd": "$ISO_CURRENT_DATE_TIME"
  }
}
END_HEREDOC
)
#datetimehour stripped from dimensions

cf_workers_json=$(
    $CURL --silent --fail --show-error --compressed \
        --request POST \
        --header "Content-Type: application/json" \
        --header "$CF_EMAIL_HEADER" \
        --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
        --data "$(echo -n $WORKERS_GRAPHQL_QUERY)" \
        "$CF_URL"
)

cf_workers_nb_errors=$(echo $cf_workers_json | $JQ ".errors | length")

if [[ $cf_workers_nb_errors -gt 0 ]]; then
    cf_workers_errors=$(echo $cf_workers_json | $JQ --raw-output ".errors[] | .message")
    printf "Cloudflare API request failed with: \n%s\nAborting\n" "$cf_workers_errors" >&2
    exit 1
fi

cf_nb_invocations=$(echo $cf_workers_json | $JQ ".data.viewer.accounts[0].workersInvocationsAdaptive | length")

if [[ $cf_nb_invocations -gt 0 ]]; then
    cf_workers_json_parsed=$(echo $cf_workers_json | $JQ ".data.viewer.accounts[0].workersInvocationsAdaptive")
    cf_stats_workers=$(
        echo "$cf_workers_json_parsed" |
            $JQ --raw-output "
        (.[] |
        [\"${CLOUDFLARE_ACCOUNT_TAG}\",
        .dimensions.scriptName,
        .dimensions.status,
        .quantiles.cpuTimeP50,
        .quantiles.cpuTimeP99,
        .quantiles.durationP50,
        .quantiles.durationP99,
        .quantiles.responseBodySizeP50,
        .quantiles.responseBodySizeP99,
        .quantiles.wallTimeP50,
        .quantiles.wallTimeP99,
        .sum.clientDisconnects,
        .sum.cpuTimeUs,
        .sum.duration,
        .sum.errors,
        .sum.requests,
        .sum.responseBodySize,
        .sum.subrequests,
        .sum.wallTime
        ])
        | @tsv" |
            $AWK '{printf "cloudflare_stats_workers,window='"${TIMESPAN}"',account=%s,worker=%s status=\"%s\",cpuTimeP50=%s,cpuTimeP99=%s,durationP50=%s,durationP99=%s,responseBodySizeP50=%s,responseBodySizeP99=%s,wallTimeP50=%s,wallTimeP99=%s,clientDisconnects=%s,cpuTimeUs=%s,duration=%s,errors=%s,requests=%s,responseBodySize=%s,subrequests=%s,wallTime=%s '"${CURRENT_UNIXTS}"'\n", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19}'
    )

        #(.dimensions.datetimeHour | fromdateiso8601) stripped from jq and awk
        echo -n  "$cf_stats_workers" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep -q ^0$ && ( echo "empty cf_stats_workers")
        echo -n  "$cf_stats_workers" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep -q ^0$ || ( echo "$cf_stats_workers" >> "${TMPDATABASE}" )
#        echo -n  "$cf_stats_workers" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep -q ^0$ || ( echo "$cf_stats_workers")
        #echo -n  "$cf_stats_workers" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( 
        #    echo "$cf_stats_workers" | sed 's/^\t\+//g;s/^ \+//g' | sed "s~$~000000000~g"| $GZIP |
        #        $CURL --silent --fail --show-error \
        #            --request POST "${INFLUXDB_URL}" \
        #            --header 'Content-Encoding: gzip' \
        #            --header "Authorization: Token $INFLUXDB_API_TOKEN" \
        #            --header "Content-Type: text/plain; charset=utf-8" \
        #            --header "Accept: application/json" \
        #             --data-binary @- )

fi

PAGES_FUNCTIONS_GRAPHQL_QUERY=$(
    cat <<END_HEREDOC
{ "query":
  "query {
    viewer {
        accounts(filter: { accountTag: \$accountTag }) {
            pagesFunctionsInvocationsAdaptiveGroups(
                filter: { datetimeHour_geq: \$datetimeStart, datetimeHour_leq: \$datetimeEnd }
                limit: 10000
            ) {
                sum {
                    clientDisconnects
                    duration
                    errors
                    requests
                    responseBodySize
                    subrequests
                    wallTime
                }
                quantiles {
                    cpuTimeP50
                    cpuTimeP99
                    durationP50
                    durationP99
                }
                dimensions {
                    scriptName
                    status
                    usageModel
                }
            }
        }
    }
}",
  "variables": {
    "accountTag": "$CLOUDFLARE_ACCOUNT_TAG",
    "datetimeStart": "$REFERENCE_DATE",
    "datetimeEnd": "$ISO_CURRENT_DATE_TIME"
  }
}
END_HEREDOC
)
#datetimehour stripped from dimensions
cf_pf_json=$(
    $CURL --silent --fail --show-error --compressed \
        --request POST \
        --header "Content-Type: application/json" \
        --header "$CF_EMAIL_HEADER" \
        --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
        --data "$(echo -n $PAGES_FUNCTIONS_GRAPHQL_QUERY)" \
        "$CF_URL"
)

cf_pf_nb_errors=$(echo $cf_pf_json | $JQ ".errors | length")

if [[ $cf_pf_nb_errors -gt 0 ]]; then
    cf_pf_errors=$(echo $cf_pf_json | $JQ --raw-output ".errors[] | .message")
    printf "Cloudflare API request failed with: \n%s\nAborting\n" "$cf_pf_errors" >&2
    exit 1
fi

cf_pf_nb_invocations=$(echo $cf_pf_json | $JQ ".data.viewer.accounts[0].pagesFunctionsInvocationsAdaptiveGroups | length")

if [[ $cf_pf_nb_invocations -gt 0 ]]; then
    cf_pf_json_parsed=$(echo $cf_pf_json | $JQ ".data.viewer.accounts[0].pagesFunctionsInvocationsAdaptiveGroups")
    cf_stats_pf=$(
        echo "$cf_pf_json_parsed" |
            $JQ --raw-output "
        (.[] |
        [\"${CLOUDFLARE_ACCOUNT_TAG}\",
        .dimensions.scriptName,
        .dimensions.status,
        .dimensions.usageModel,
        .quantiles.cpuTimeP50,
        .quantiles.cpuTimeP99,
        .quantiles.durationP50,
        .quantiles.durationP99,
        .sum.clientDisconnects,
        .sum.duration,
        .sum.errors,
        .sum.requests,
        .sum.responseBodySize,
        .sum.subrequests,
        .sum.wallTime
        ])
        | @tsv" |
            $AWK '{printf "cloudflare_stats_pf,window='"${TIMESPAN}"',account=%s,scriptName=%s status=\"%s\",usageModel=\"%s\",cpuTimeP50=%s,cpuTimeP99=%s,durationP50=%s,durationP99=%s,clientDisconnects=%s,duration=%s,errors=%s,requests=%s,responseBodySize=%s,subrequests=%s,wallTime=%s '"${CURRENT_UNIXTS}"'\n", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15}'
    )
## stripped datetime |fromiso8601 from jq and awk
        echo -n  "$cf_stats_pf" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ && ( echo "empty cf_stats_pf")
        echo -n  "$cf_stats_pf" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( echo "$cf_stats_pf" >> "${TMPDATABASE}" )
        #echo -n  "$cf_stats_pf" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( 
        #    echo "$cf_stats_pf" | sed 's/^\t\+//g;s/^ \+//g' | sed "s~$~000000000~g"| $GZIP |
        #        $CURL --silent --fail --show-error \
        #            --request POST "${INFLUXDB_URL}" \
        #            --header 'Content-Encoding: gzip' \
        #            --header "Authorization: Token $INFLUXDB_API_TOKEN" \
        #            --header "Content-Type: text/plain; charset=utf-8" \
        #            --header "Accept: application/json" \
        #             --data-binary @- )
fi

if [[ -n "${CLOUDFLARE_KV_NAMESPACES}" ]]; then

    for kv_namespace_id in $(echo "${CLOUDFLARE_KV_NAMESPACES}"); do
        KV_GRAPHQL_QUERY=$(
            cat <<END_HEREDOC
{ "query":
  "query {
    viewer {
        accounts(filter: { accountTag: \$accountTag }) {
            kvOperationsAdaptiveGroups(
                filter: { namespaceId: \$namespaceId, datetimeHour_geq: \$datetimeStart, datetimeHour_leq: \$datetimeEnd }
                limit: 10000
            ) {
                sum {
                    objectBytes
                    requests
                }
                quantiles {
                    latencyMsP50
                    latencyMsP99
                }
                dimensions {
                    actionType
                    namespaceId
                    responseStatusCode
                    result
                }
            }
        }
    }
}",
  "variables": {
    "accountTag": "$CLOUDFLARE_ACCOUNT_TAG",
    "namespaceId": "$kv_namespace_id",
    "datetimeStart": "$REFERENCE_DATE",
    "datetimeEnd": "$ISO_CURRENT_DATE_TIME"
  }
}
END_HEREDOC
        )
#datetimehour stripped from dimensions

        cf_kv_json=$(
            $CURL --silent --fail --show-error --compressed \
                --request POST \
                --header "Content-Type: application/json" \
                --header "$CF_EMAIL_HEADER" \
                --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
                --data "$(echo -n $KV_GRAPHQL_QUERY)" \
                "$CF_URL"
        )


        cf_kv_nb_errors=$(echo $cf_kv_json | $JQ ".errors | length")

        if [[ $cf_kv_nb_errors -gt 0 ]]; then
            cf_kv_errors=$(echo $cf_kv_json | $JQ --raw-output ".errors[] | .message")
            printf "Cloudflare API request failed with: \n%s\nAborting\n" "$cf_kv_errors" >&2
            exit 1
        fi

        cf_kv_json_parsed=$(echo $cf_kv_json | $JQ ".data.viewer.accounts[0].kvOperationsAdaptiveGroups")
        cf_stats_kv=$(
            echo "$cf_kv_json_parsed" |
                $JQ --raw-output "
        (.[] |
        [\"${CLOUDFLARE_ACCOUNT_TAG}\",
        .dimensions.namespaceId,
        .dimensions.actionType,
        .dimensions.result,
        .dimensions.responseStatusCode,
        .quantiles.latencyMsP50,
        .quantiles.latencyMsP99,
        .sum.objectBytes,
        .sum.requests
        ])
        | @tsv" |
                $AWK '{printf "cloudflare_stats_kv_ops,window='"${TIMESPAN}"',account=%s,namespace=%s actionType=\"%s\",result=\"%s\",responseStatusCode=%s,latencyMsP50=%s,latencyMsP99=%s,objectBytes=%s,requests=%s '"${CURRENT_UNIXTS}"'\n", $1, $2, $3, $4, $5, $6, $7, $8, $9}'
        )
# stripped         (.dimensions.datetimeHour | fromdateiso8601) from jq and awk
        echo -n  "$cf_stats_kv" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ && ( echo "empty cf_stats_kv")
        echo -n  "$cf_stats_kv" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( echo "$cf_stats_kv" >> ${TMPDATABASE} )

        #echo -n  "$cf_stats_kv" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( 
        #    echo "$cf_stats_kv" | sed 's/^\t\+//g;s/^ \+//g' | sed "s~$~000000000~g"| $GZIP |
        #        $CURL --silent --fail --show-error \
        #            --request POST "${INFLUXDB_URL}" \
        #            --header 'Content-Encoding: gzip' \
        #            --header "Authorization: Token $INFLUXDB_API_TOKEN" \
        #            --header "Content-Type: text/plain; charset=utf-8" \
        #            --header "Accept: application/json" \
        #             --data-binary @- )
#
        KV_STORAGE_GRAPHQL_QUERY=$(
            cat <<END_HEREDOC
{ "query":
  "query {
    viewer {
        accounts(filter: { accountTag: \$accountTag }) {
            kvStorageAdaptiveGroups(
                filter: { namespaceId: \$namespaceId, datetimeHour_geq: \$datetimeStart, datetimeHour_leq: \$datetimeEnd }
                limit: 10000
            ) {
                max {
                    keyCount
                    byteCount
                }
                dimensions {
                    namespaceId
                }
            }
        }
    }
}",
  "variables": {
    "accountTag": "$CLOUDFLARE_ACCOUNT_TAG",
    "namespaceId": "$kv_namespace_id",
    "datetimeStart": "$REFERENCE_DATE",
    "datetimeEnd": "$ISO_CURRENT_DATE_TIME"
  }
}
END_HEREDOC
        )
#datetimehour stripped from dimensions

        cf_kv_storage_json=$(
            $CURL --silent --fail --show-error --compressed \
                --request POST \
                --header "Content-Type: application/json" \
                --header "$CF_EMAIL_HEADER" \
                --header "Authorization: Bearer $CLOUDFLARE_API_TOKEN" \
                --data "$(echo -n $KV_STORAGE_GRAPHQL_QUERY)" \
                "$CF_URL"
        )

        cf_kv_storage_nb_errors=$(echo $cf_kv_storage_json | $JQ ".errors | length")

        if [[ $cf_kv_storage_nb_errors -gt 0 ]]; then
            cf_kv_storage_errors=$(echo $cf_kv_storage_json | $JQ --raw-output ".errors[] | .message")
            printf "Cloudflare API request failed with: \n%s\nAborting\n" "$cf_kv_storage_errors" >&2
            exit 1
        fi

        cf_kv_storage_json_parsed=$(echo $cf_kv_storage_json | $JQ ".data.viewer.accounts[0].kvStorageAdaptiveGroups")
        cf_stats_kv_storage=$(
            echo "$cf_kv_storage_json_parsed" |
                $JQ --raw-output "
        (.[] |
        [\"${CLOUDFLARE_ACCOUNT_TAG}\",
        .dimensions.namespaceId,
        .max.byteCount,
        .max.keyCount
        ])
        | @tsv" |
                $AWK '{printf "cloudflare_stats_kv_storage,window='"${TIMESPAN}"',account=%s,namespace=%s byteCount=%s,keyCount=%s '"${CURRENT_UNIXTS}"'\n", $1, $2, $3, $4}'
        )
# stripped         (.dimensions.datetimeHour | fromdateiso8601) from jq and awk



        # orig script uses seconds 
        # orig script triggered empty values and possibly wrong values with trailing space/tab
        echo -n  "$cf_stats_kv_storage" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ && ( echo "empty cf_stats_pf")
        echo -n  "$cf_stats_kv_storage" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( echo "$cf_stats_kv_storage" >> ${TMPDATABASE} )
#        echo -n  "$cf_stats_kv_storage" | sed 's/^\t\+//g;s/^ \+//g' |wc -c|grep ^0$ || ( 
#            echo "$cf_stats_kv_storage" | sed 's/^\t\+//g;s/^ \+//g' | sed "s~$~000000000~g"| $GZIP |
#                $CURL --silent --fail --show-error \
#                    --request POST "${INFLUXDB_URL}" \
#                    --header 'Content-Encoding: gzip' \
#                    --header "Authorization: Token $INFLUXDB_API_TOKEN" \
#                    --header "Content-Type: text/plain; charset=utf-8" \
#                    --header "Accept: application/json" \
#                     --data-binary @- )

    done

fi

GZIPHEADER='--header "Content-Encoding: gzip"'
datapipe() { 
    $GZIP
}
[[ "$NOGZIP" = "true" ]] && GZIPHEADER=""
[[ "$NOGZIP" = "true" ]] && datapipe() { cat ; } ; 
CONTENTHEADER='--header "Content-Type: text/plain; charset=utf-8" '
[[ "${CONTENTJSON}" = "true" ]] && CONTENTHEADER="--header 'Content-Type: application/json' "
cat  "${TMPDATABASE}"| sed 's/^\t\+//g;s/^ \+//g' |grep -v ^$|wc -l 
cat "${TMPDATABASE}"| sed 's/^\t\+//g;s/^ \+//g' |grep -v ^$|wc -l |grep -q ^0$ && { echo "EMPTY_DB"; exit 1 ; } ;


OPTIONSSTRING=""
[[ -z "$SOCKSURL" ]]      || OPTIONSSTRING=" -x socks5h://${SOCKSURL} "
[[ -z "$GZIPHEADER" ]]    || OPTIONSSTRING="${OPTIONSSTRING} ""${GZIPHEADER} "
[[ -z "$CONTENTHEADER" ]] || OPTIONSSTRING="${OPTIONSSTRING} ""${CONTENTHEADER} "

cat "${TMPDATABASE}"| sed 's/^\t\+//g;s/^ \+//g' |grep -v ^$| sed "s~$~000000000~g"| datapipe >"${TMPDATABASE}.send"
sendcommand=$(echo $CURL --silent --fail --show-error --request POST "'""${INFLUXDB_URL}""'" --header "'Authorization: ""${INFLUXAUTHSTRING}""'" $(echo $OPTIONSSTRING )  \
                    '--header "Accept: application/json"' \
                     --data-binary  @- )
echo "$sendcommand"
cat "${TMPDATABASE}.send" | ( echo "$sendcommand"|bash  )||exit 2
#                     --data-binary @- 