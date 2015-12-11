redis.replicate_commands()
local __RETVAL = function(value, retval)
  local __RESULT = {}
  __RESULT["__value"] = value
  __RESULT["__return"] = retval

  return cmsgpack.pack(__RESULT)
end

local __TRUE = function(expr)
  local __VAL = expr
  if not __VAL or __VAL == 0 then
    return false
  end

  local __TYPE = type(__VAL)
  if (__TYPE == "table" or __TYPE == "string") and #__VAL == 0 then
      return false
  else
      return true
  end
end

local __OR = function(...)
    local __VAL = nil
    for i, v in ipairs(arg) do
        if __TRUE(v) then
            return v
        else
            __VAL = v
        end
    end

    return __VAL
end

local __AND = function(...)
    local __VAL = nil
    for i, v in ipairs(arg) do
        if not __TRUE(v) then
            return v
        else
            __VAL = v
        end
    end

    return __VAL
end
