<?php

function errorlog_provision_apache_vhost_config($uri, $data) {
  return "ErrorLog '|$/bin/su aegir -c /usr/local/bin/pipeout'";
}
