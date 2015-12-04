redis.replicate_commands()
local __RETVAL = function(value, retval)
  local __RESULT = {}
  __RESULT["__value"] = value
  __RESULT["__return"] = retval

  return cmsgpack.pack(__RESULT)
end
