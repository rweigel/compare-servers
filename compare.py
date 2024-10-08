import os
import copy
import time
import json
import datetime
from urllib.parse import urlparse

import requests
import requests_cache
import urllib3
import deepdiff

from hapiclient import hapitime2datetime
import utilrsw

def cli(config):
  data_dir = os.path.join(os.path.dirname(__file__), 'data')

  clkws = {
    "id": {
      "help": "Pattern for dataset IDs to include, e.g., '^A|^B' (default: .*)"
    },
    "include": {
      "help": "(Deprecated) Same as --id"
    },
    "mode": {
      "help": "'update' or 'exact'; if 'update', warnings for backwards compatible differences",
      "default": "update"
    },
    "conf": {
      "help": "Configuration options to use from compare.json (default: %(default)s)",
      "default": "CDAWeb",
      "choices": list(config.keys())
    },
    "warn": {
      "action": "store_true",
      "help": "Print warnings",
      "default": False
    },
    "data-dir": {
      "help": "Data directory",
      "default": data_dir
    },
    "parallel": {
      "action": "store_true",
      "help": "Make parallel requests",
      "default": False
    },
    "compare-data": {
      "action": "store_true",
      "help": "Compare data",
      "default": False
    },
    "log-level": {
      "help": "Log level",
      "default": 'info',
      "choices": ['debug', 'info', 'warning', 'error', 'critical']
    },
    "debug": {
      "action": "store_true",
      "help": "Same as --log-level debug",
      "default": False
    }
  }

  import argparse
  parser = argparse.ArgumentParser()
  for k, v in clkws.items():
    parser.add_argument(f'--{k}', **v)

  # Note that hyphens are converted to underscores when parsing
  args = vars(parser.parse_args())
  if args['conf'] == 'CDAWeb-metadata' and args['compare_data']:
    raise ValueError("CDAWeb-metadata does not support comparing data.")

  if args['debug']:
    args['log_level'] = 'debug'

  args['data_dir'] = os.path.abspath(args['data_dir'])

  return args

def _logger(log_level, data_dir, conf_name):
  logger_ = {
      "name": "compare",
      "file_log": f"{data_dir}/compare.{conf_name}.log",
      "file_error": f"{data_dir}/compare.{conf_name}.error.log",
      "console_format": "%(name)s %(levelname)s %(message)s",
      "color": True,
      "debug_logger": False
  }

  if log_level.lower() == 'debug':
    logger_["console_format"] = "%(name)s %(levelname)s %(filename)s:%(lineno)d %(message)s"

  logger = utilrsw.logger(**logger_)
  logger.setLevel(args['log_level'].upper())

  return logger

def omit(id):
  # TODO: This is a copy of function in cdaweb.py. Move to a common module.
  import re
  if id == 'AIM_CIPS_SCI_3A':
    return True
  if opts['id'] is not None:
    if re.search(opts['id'], id):
      return False
    return True
  else:
    return False

def compare_metadata(datasets_s1, datasets_s2, opts):

  indent = '  '

  for dsid in datasets_s2.keys():
    if omit(dsid):
      continue

    if dsid not in datasets_s1:
      msg = f"Not in {opts['s1']}"
      if opts['mode'] == 'update':
        if opts['warn']:
          logger.info(f"{dsid}")
          logger.warning(indent + msg)
      else:
        logger.info(f"{dsid}")
        logger.error(indent + msg)
      dsid0 = dsid + "@0"
      if dsid[-2] != "@" and dsid0 in list(datasets_s1.keys()):
        logger.error(f"{indent}But {dsid0} in {opts['s1']}")

  for dsid in datasets_s1.keys():

    if omit(dsid):
      continue

    logger.info(f"{dsid}")

    extra = ""
    if "x_cdf_depend_0_name" in datasets_s1[dsid]["info"]["parameters"][0]:
      # Special case for when s1 = 'bw'
      x_cdf_depend_0_name = datasets_s1[dsid]["info"]["parameters"][0]["x_cdf_depend_0_name"]
      extra = f'for s1 DEPEND_0 = {x_cdf_depend_0_name}'

    if dsid not in datasets_s2:

      logger.error(f"{indent}{dsid} not in {opts['s2']} {extra}")
      dsid0 = dsid + "@0"
      if dsid[-2] != "@" and dsid0 in list(datasets_s2.keys()):
        logger.error(f"{indent}But {dsid0} in {opts['s2']}")

    else:

      compare_info(dsid, datasets_s2[dsid]["info"], datasets_s1[dsid]["info"])

      keys_s2 = datasets_s2[dsid]["info"]["_parameters"].keys()
      keys_s1 = datasets_s1[dsid]["info"]["_parameters"].keys()

      n_params_s2 = len(keys_s2)
      n_params_s1 = len(keys_s1)

      if n_params_s2 != n_params_s1:
        m = min(n_params_s2, n_params_s1)
        if list(keys_s1)[0:m] != list(keys_s2)[0:m]:
          logger.error(f"{indent}n_params_{opts['s2']} = {n_params_s2} != n_params_{opts['s1']} = {n_params_s1} {extra}")
          logger.error(f"{2*indent}Differences: {set(keys_s1) ^ set(keys_s2)}")
          logger.error(f"{2*indent}Error because first {m} parameters are not identical.")
        else:
          msgo = f"{2*indent}n_params_{opts['s2']} = {n_params_s2} > n_params_{opts['s1']} = {n_params_s1}. {extra}"
          msgw = f"{3*indent}Warning b/c first {m} parameters are same & mode = 'update'"
          msgx = f"{3*indent}Differences: {set(keys_s1) ^ set(keys_s2)}"
          if n_params_s2 > n_params_s1:
            if opts['mode'] != 'update':
              logger.error(msgo)
              logger.error(msgx)
            else:
              logger.warning(msgo)
              logger.warning(msgw)
              logger.warning(msgx)
          else:
            msgo = msgo.replace(" > ", " < ")
            if opts['mode'] != 'update':
              logger.error(msgo)
              logger.error(msgx)
            else:
              logger.warning(msgo)
              logger.warning(msgw)
              logger.warning(msgx)

          parameters = list(keys_s1)[0:m]
          compare_data(dsid, datasets_s1, datasets_s2, opts, parameters=parameters)
      else:
        if keys_s2 != keys_s1:
          logger.error(f'{indent}Order differs {extra}','fail')
          logger.error(f"{2*indent}{opts['s2_padded']}: {list(keys_s2)}",'info')
          logger.error(f"{2*indent}{opts['s1_padded']}: {list(keys_s1)}",'info')
        else:
          for i in range(len(datasets_s2[dsid]["info"]["parameters"])):
            param_s2 = datasets_s2[dsid]["info"]["parameters"][i]
            param_s1 = datasets_s1[dsid]["info"]["parameters"][i]
            compare_parameter(dsid, param_s2, param_s1)

          compare_data(dsid, datasets_s1, datasets_s2, opts)

def compare_info(dsid, info_s2, info_s1):

  indent = "  "
  keys_s1 = list(info_s1.keys())
  keys_s2 = list(info_s2.keys())

  keys_s1 = remove_keys(keys_s1, 's1', opts)
  keys_s2 = remove_keys(keys_s2, 's2', opts)

  n_keys_s2 = len(keys_s2)
  n_keys_s1 = len(keys_s1)

  if n_keys_s2 != n_keys_s1:
    logger.error(f'{indent}n_keys_{opts["s2"]} = {n_keys_s2} != n_keys_{opts["s1"]} = {n_keys_s1}')
    logger.error(f"{2*indent}Differences: {set(keys_s1) ^ set(keys_s2)}")
  else:
    common_keys = set(keys_s2) & set(keys_s1)
    for key in common_keys:
      if info_s2[key] != info_s1[key]:
        if key.endswith('Date'):
          date1 = hapitime2datetime(info_s1[key])[0]
          date2 = hapitime2datetime(info_s2[key])[0]
          if date1 != date2:
            msg = f'{indent}{key} (datetime comparison) val_{opts["s2"]} = {info_s2[key]} '
            msg += '!= val_{opts["s1"]} = {info_s1[key]}'
            logger.error(msg)
          elif args['warn']:
            msg = f'{indent}{key} val_{opts["s2"]} = {info_s2[key]} != '
            msg += 'val_{opts["s1"]} = {info_s1[key]} but datetime equivalent.'
            logger.warning(msg)
        else:
          msg = f'{indent}{key} val_{opts["s2"]} = {info_s2[key]} != val_{opts["s1"]} = {info_s1[key]}'
          logger.error(msg)

def compare_parameter(dsid, param_s2, param_s1):

  indent = "  "
  param_s1_keys = sorted(list(param_s1.keys()))
  param_s2_keys = sorted(list(param_s2.keys()))

  for key in param_s1_keys.copy():
    if key.startswith("x_"):
      param_s1_keys.remove(key)

  for key in param_s2_keys.copy():
    if key.startswith("x_"):
      param_s2_keys.remove(key)

  n_param_keys_s1 = len(param_s1_keys)
  n_param_keys_s2 = len(param_s2_keys)
  if n_param_keys_s1 != n_param_keys_s2:
    if {'bins'} != set(param_s1_keys) ^ set(param_s2_keys):
      logger.info(f"{param_s2['name']}")
      msg = f"{2*indent}n_param_keys_{opts['s2']} = {n_param_keys_s2} != "
      msg += "n_param_keys_{opts['s1']} = {n_param_keys_s1}"
      logger.error(msg)
      logger.error(f"{3*indent}Differences: {set(param_s1_keys) ^ set(param_s2_keys)}")

  common_keys = set(param_s2_keys) & set(param_s1_keys)

  for key in common_keys:
    if key == 'bins':
      # Checked later
      continue
    if param_s2[key] != param_s1[key]:
      msgo = f"{indent}{param_s2['name']}/{key}"
      if key == 'fill' and 'type' in param_s2 and 'type' in param_s1:
        a = (param_s2['type'] == 'int' or param_s2['type'] == 'double')
        b = (param_s1['type'] == 'int' or param_s1['type'] == 'double')
        if a and b:
          if float(param_s2[key]) != float(param_s1[key]):
            logger.info(msgo)
            msg = f"{2*indent}val_{opts['s2']} = {param_s2[key]} != val_{opts['s1']} = {param_s1[key]}"
            if opts['mode'] == 'update':
              logger.warning(msg)
            else:
              logger.error(msg)
      elif key == 'size' and isinstance(param_s2[key],list) and isinstance(param_s1[key], list):
        if param_s2[key] != param_s1[key]:
          logger.info(msgo)
          logger.info(f"{2*indent}val_{opts['s2']} = {param_s2[key]} != val_{opts['s1']} = {param_s1[key]}")
      elif type(param_s2[key]) != type(param_s1[key]):
        logger.info(msgo)
        msg = f"{2*indent}type_{opts['s2']} = {type(param_s2[key])} != type_{opts['s1']} = {type(param_s1[key])}"
        if opts['mode'] == 'update':
          logger.warning(msg)
        else:
          logger.error(msg)
      else:
        logger.info(msgo)
        if key == 'description':
          msg1 = f"{2*indent}val_{opts['s2']} = '{param_s2[key]}'"
          msg2 = f"{2*indent}!="
          msg3 = f"{2*indent}val_{opts['s1']} = '{param_s1[key]}'"
          if opts['mode'] == 'update':
            logger.warning(msg1)
            logger.warning(msg2)
            logger.warning(msg3)
          else:
            logger.error(msg1)
            logger.error(msg2)
            logger.error(msg3)
        else:
          msg = f"{2*indent}val_{opts['s2']} = '{param_s2[key]}' != val_{opts['s1']} = '{param_s1[key]}'"
          if opts['mode'] == 'update':
            logger.warning(msg)
          else:
            logger.error(msg)

  compare_bins(param_s2, param_s1)

def compare_bins(params_s2, params_s1):

  name_s2 = params_s2["name"]
  name_s1 = params_s1["name"]
  if 'bins' in params_s2:
    if 'bins' not in params_s1:
      logger.info(f"  {name_s2}")
      msg = f"{opts['s2']} has bins for '{name_s2}' but {opts['s1']} does not"
      if opts['mode'] == 'update':
        logger.warning(msg)
      else:
        logger.error(msg)
  if 'bins' in params_s1:
    if 'bins' not in params_s2:
      logger.error(f"  {name_s1}")
      logger.error(f"{opts['s1']} has bins for '{name_s1}' but {opts['s2']} does not")
  if 'bins' in params_s1:
    if 'bins' in params_s2:
      n_bins_s2 = len(params_s1["bins"])
      n_bins_s1 = len(params_s2["bins"])
      if n_bins_s2 != n_bins_s1:
        logger.error(f"  {params_s1}/bins")
        logger.error(f"{opts['s1']} has {n_bins_s1} bins objects; {opts['s2']} has {n_bins_s2}")
      # TODO: Compare content at bins level

def compare_data(dsid, datasets_s1, datasets_s2, opts, parameters=""):

  if opts['compare_data'] is False:
    return
  if dsid not in datasets_s1:
    return
  if dsid not in datasets_s2:
    return

  sampleStartDate = None
  sampleStopDate = None

  if 'sampleStartDate' in datasets_s2[dsid]['info']:
    sampleStartDate = datasets_s2[dsid]['info']['sampleStartDate']
  if 'sampleStartDate' in datasets_s1[dsid]['info']:
    sampleStartDate = datasets_s1[dsid]['info']['sampleStartDate']

  if 'sampleStopDate' in datasets_s2[dsid]['info']:
    sampleStopDate = datasets_s2[dsid]['info']['sampleStopDate']
  if 'sampleStartDate' in datasets_s1[dsid]['info']:
    sampleStopDate = datasets_s1[dsid]['info']['sampleStopDate']

  if sampleStartDate is None or sampleStopDate is None:
    startDate_s1 = hapitime2datetime(datasets_s1[dsid]['info']['startDate'])[0]
    startDate_s2 = hapitime2datetime(datasets_s2[dsid]['info']['startDate'])[0]
    if startDate_s1 != startDate_s2:
      sampleStartDate = max(startDate_s1, startDate_s2)
    else:
      sampleStartDate = startDate_s1

    sampleStopDate = sampleStartDate + datetime.timedelta(**opts['sample_duration'])

    sampleStartDate = sampleStartDate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    sampleStopDate = sampleStopDate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  times = 2*[None]
  resps = 2*[None]

  urlo = "/data?id=" + dsid \
        + "&parameters=" + ",".join(parameters) \
        + "&time.min=" + sampleStartDate \
        + "&time.max=" + sampleStopDate

  urls = [opts['url1'] + urlo, opts['url2'] + urlo]

  def get(i):
    start = time.time()
    logger.info("  Getting: " + urls[i])
    resps[i] = requests.get(urls[i], verify=False)
    times[i] = time.time() - start

  logger.info(f"{dsid} - Checking data")

  if opts['parallel'] is False:
    get(0)
    get(1)
  else:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
      pool.map(get, range(2))

  dt1 = "{0:.6f}".format(times[0])
  msg1 = f"  {opts['s1_padded']} time = {dt1} [s]; status = {resps[0].status_code}"
  if resps[0].status_code != 200:
    logger.error(msg1)
  else:
    logger.info(msg1)

  dt2 = "{0:.6f}".format(times[1])
  msg2 = f"  {opts['s2_padded']} time = {dt2} [s]; status = {resps[1].status_code}"
  if resps[1].status_code != 200:
    logger.error(msg2)
  else:
    logger.info(msg2)

  if resps[0].status_code != resps[1].status_code:
    logger.error(f"  {opts['s2']} HTTP status = {resps[1].status_code} != {opts['s1']} HTTP status = {resps[0].status_code}")
    return

  after =  "after replacement of '\\r\\n' with '\\n' and trimming trailing whitespace."

  body1 = resps[0].text
  body2 = resps[1].text
  body1r = body1.replace("\r\n", "\n").rstrip()
  body2r = body2.replace("\r\n", "\n").rstrip()

  delta1 = len(body1) - len(body1r)
  if delta1 != 0:
    logger.warning(f"  {opts['s1']} data length changed by {delta1} {after}")
  delta2 = len(body2) - len(body2r)
  if delta2 != 0:
    logger.warning(f"  {opts['s2']} data length changed by {delta2} {after}")

  if len(body1) != len(body2) and len(body1r) != len(body2r):
    logger.error(f"  {opts['s2']} data (length = {len(body2)}) != {opts['s1']} data (length = {len(body1)})")
    logger.error("  and")
    logger.error(f"  {opts['s2']} data (length = {len(body2r)}) != {opts['s1']} data (length = {len(body1r)}) {after}")

  if body1r != body2r:
    body1s = body1r.splitlines()
    body2s = body2r.splitlines()
    if body1s == body2s:
      logger.info(f"  {opts['s2']} data == {opts['s1']} data after splitlines()")

    if len(body1s) == len(body2s):
      logger.info(f"  {opts['s2']} data has {len(body2s)} lines; {opts['s1']} data has {len(body1s)} lines {after}")
    else:
      logger.error(f"  {opts['s2']} data has {len(body2s)} lines; {opts['s1']} data has {len(body1s)} lines {after}")

    n = 0
    for i in range(min(len(body1s), len(body2s))):
      if body1s[i] != body2s[i]:
        msg = f"  Line {i}:"
        logger.error(msg)
        logger.error(f"    {opts['s1_padded']}: {body1s[i]}")
        logger.error(f"    {opts['s2_padded']}: {body2s[i]}")
        n += 1
        if n > 10:
          logger.error("  More than 10 lines differ; not displaying more.")
          break

def remove_keys(keys, s, opts):
  for key in keys.copy():
    if f'{s}_omits' in opts and key in opts[f'{s}_omits']:
      keys.remove(key)
    if key.startswith("x_"):
      keys.remove(key)
    if key in ['_parameters', 'parameters']:
      keys.remove(key)
  return keys

def get_all_metadata(server_url, server_name, expire_after={"days": 1}):

  if expire_after is None:
    expire_after = {"days": 0}

  # Could do these in parallel.

  def server_dir(url):
    url_parts = urlparse(url)
    url_dir = os.path.join(opts['data_dir'], 'CachedSession', 'compare', url_parts.netloc, *url_parts.path.split('/'))
    os.makedirs(url_dir, exist_ok=True)
    return url_dir

  if not server_url.startswith('http'):
    logger.info(f"Reading: {server_url}")
    with open(server_url, 'r', encoding='utf-8') as f:
      datasets = json.load(f)
    logger.info(f"Read: {server_url}")
    return datasets

  cache_dir = server_dir(server_url)
  logger.info("Getting catalog and info metadata")

  def CachedSession():
    # https://requests-cache.readthedocs.io/en/stable/#settings
    # https://requests-cache.readthedocs.io/en/stable/user_guide/headers.html
    copts = {
      "cache_control": True,                # Use Cache-Control response headers for expiration, if available
      "expire_after": datetime.timedelta(**expire_after), # Otherwise expire after this
      "allowable_codes": [200],             # Cache responses with these status codes
      "stale_if_error": False,              # In case of request errors, use stale cache data if possible
      "backend": "filesystem"
    }
    return requests_cache.CachedSession(cache_dir, **copts)

  session = CachedSession()

  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
  resp = session.request('get', server_url + '/catalog', verify=False)
  datasets = resp.json()['catalog']

  for dataset in datasets:
    id = dataset['id']
    if omit(id):
      continue

    url = server_url + '/info?id=' + id

    start = time.time()
    logger.info(f'  Getting {server_name}: {url}')
    resp = session.request('get', url, verify=False)
    if resp.from_cache:
      logger.info(f'  Got: (from cache) {url}')
      file_cache = os.path.join(cache_dir, resp.cache_key + ".json")
      logger.info(f'  Cache file: {file_cache}')
    else:
      dt = "{0:.6f}".format(time.time() - start)
      logger.info(f'  Got: (time = {dt} [s]) {url}')

    if resp.status_code != 200:
      continue

    dataset['info'] = resp.json()
    del dataset['info']['status']
    del dataset['info']['HAPI']

  return datasets

def restructure(datasets, svr):
  """Create _parameters dict with keys of parameter name."""
  datasetsr = {}
  for dataset in datasets:
    id = dataset["id"]
    if omit(id):
      continue
    if "info" not in dataset:
      logger.error(f"Dataset {id} in {svr} has no info")
      continue

    datasetsr[id] = copy.deepcopy(dataset)
    datasetsr[id]["info"]["_parameters"] = {}
    for parameter in dataset["info"]["parameters"]:
      name = parameter["name"]
      datasetsr[id]["info"]["_parameters"][name] = parameter
  return datasetsr

def pad_server_name(opts):
  l1 = len(opts['s1'])
  l2 = len(opts['s2'])
  opts['s2_padded'] = opts['s2']
  opts['s1_padded'] = opts['s1']
  if l1 > l2:
    opts['s2_padded'] = opts['s2'] + ' '*(l1-l2)
  if l2 > l1:
    opts['s1_padded'] = opts['s1'] + ' '*(l2-l1)

  return opts

# Read configuration
fname = os.path.join(os.path.dirname(__file__), 'compare.json')
config = utilrsw.read(fname)

# Read command line arguments
args = cli(config)
opts = config[args['conf']]
opts['data_dir'] = args['data_dir']
opts.update(args)

logger = _logger(args['log_level'], args['data_dir'], args['conf'])
logger.info(f"Logging output to {opts['data_dir']}")
logger.info(f"Cache directory: {opts['data_dir']}")

opts = pad_server_name(opts)

if opts['include']:
  logger.warning("--include is deprecated. Use --id")
  opts['id'] = opts['include']

if opts['mode'] == 'update':
  msg = "--mode = 'update'; Backward compatible differences will be treated as warnings."
  logger.info(msg)

if opts['mode'] == 'update':
  logger.info("Original server")
logger.info(f"  {opts['s1']} = {opts['url1']}")

if opts['mode'] == 'update':
  logger.info("Updated server")
logger.info(f"  {opts['s2']} = {opts['url2']}")

cache1 = f"{opts['data_dir']}/cache/catalog-all.{opts['s1']}.pkl"
cache2 = f"{opts['data_dir']}/cache/catalog-all.{opts['s2']}.pkl"

if opts['id'] is None and opts['s1_expire_after'] is None and os.path.exists(cache1):
  datasets_s1o = utilrsw.read(cache1, logger=logger)
else:
  datasets_s1o = get_all_metadata(opts['url1'], opts['s1'], expire_after=opts['s1_expire_after'])

if opts['id'] is None and opts['s2_expire_after'] is None and os.path.exists(cache2):
  datasets_s2o = utilrsw.read(cache2, logger=logger)
else:
  datasets_s2o = get_all_metadata(opts['url2'], opts['s2'], expire_after=opts['s2_expire_after'])

if opts['id'] is None:
  utilrsw.write(cache1, datasets_s1o, logger=logger)
  utilrsw.write(cache2, datasets_s2o, logger=logger)

logger.info("")

if False:
  if {} == deepdiff.DeepDiff(datasets_s1o, datasets_s2o):
    # Check to see if we abort metadata checks early.
    # Takes a long time for large catalogs.
    logger.info("All /info metadata is the same.")
    if opts['compare_data'] is False:
      exit(0)

datasets_s1 = restructure(datasets_s1o, opts['s1'])
datasets_s2 = restructure(datasets_s2o, opts['s2'])

compare_metadata(datasets_s1, datasets_s2, opts)
