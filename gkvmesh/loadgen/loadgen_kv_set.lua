local mime = require('mime')

math.randomseed(os.time())

local charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

local function randomString(length)
  if length > 0 then
    local index = math.random(1, #charset)
    return randomString(length - 1) .. charset:sub(index, index)
  else
    return ""
  end
end

wrk.method = "POST"
wrk.headers["Content-type"] = "application/json"

local keyCount = 100000
local valueCount = 1000
local keys = {}
local values = {}

init = function(args)
  for i = 1, keyCount do
    keys[i] = randomString(10)
  end

  for i = 1, valueCount do
    values[i] = randomString(100)
  end
end

request = function()
  local key = keys[math.random(1, keyCount)]
  local value = values[math.random(1, valueCount)]
  return wrk.format(wrk.method, nil, wrk.headers, '{"key":"' .. mime.b64(key) .. '","value":"' .. mime.b64(value) .. '"}')
end
