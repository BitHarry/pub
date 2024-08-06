machines=$(sql2 --format=no-header,no-commas  -q map.devbl.query.akadns.net  "SELECT distinct ip from mcm_machines WHERE network = 'brave' and regionname like '%FABRIC%'" |xargs);
date=$( date +%s);
cmd="ghost_grep --use-normandy --range=$date --log-file=arcd.aggregatedconns.log  \".\" $machines >   ${date}.arcdaggconn.log";
echo $cmd;
eval $cmd;