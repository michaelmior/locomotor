local __PIPELINE_RESULTS = {}

local __PIPE_ADD = function(key, value)
  if __PIPELINE_RESULTS[key] == nil then
    __PIPELINE_RESULTS[key] = {}
  end

  table.insert(__PIPELINE_RESULTS[key], value)
  return value
end

local __PIPE_GET = function(key)
  local RETVAL = __PIPELINE_RESULTS[key]
  __PIPELINE_RESULTS[key] = {}
  return RETVAL
end

