local redis = require 'redis'
 
local host = "127.0.0.1"
local port = 6379

client = redis.connect(host, port)

-- redis.replicate_commands()

local tpcc = function(params)
  local self = {}

  self.safeKey = function(keys)
    local new_keys = {};
    for _, k in ipairs(keys) do
      table.insert(new_keys, tostring(k))
    end

    local key
    key, _ = string.gsub(string.gsub(table.concat(new_keys, ':'), '\n', ''), ' ', '')
    return key
  end

  local c_id;
  local no_key;
  local customer_key;
  local no_o_id;
  local new_balance;
  local result;
  local si_key;
  local id_set;
  local order_key;
  local index;
  local no_si_key;
  local ol_delivery_d;
  local index_key;
  local counter;
  local ol_ids;
  local old_balance;
  local ol_counts;
  local w_id;
  local pipe_results;
  local ol_total;
  local o_carrier_id;

  w_id = params['w_id'];
  o_carrier_id = params['o_carrier_id'];
  ol_delivery_d = params['ol_delivery_d'];

  result = {};
  order_key = {};
  ol_total = {};
  customer_key = {};
  ol_counts = {};
  no_o_id = {};

  for d_id=1, 10 + 1 - 1, 1 do
    table.insert(order_key, nil)
    table.insert(ol_total, 0)
    table.insert(customer_key, nil)
    table.insert(ol_counts, 0)
  end

  id_set = client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      index_key = self.safeKey({d_id, w_id});
      p:srandmember('NEW_ORDER.INDEXES.GETNEWORDER.' .. index_key)
    end
  end)

  no_o_id = client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if (not id_set[d_id]) then
        p:get('NULL_VALUE')
      else
        p:hget('NEW_ORDER.' .. tostring(id_set[d_id]), 'NO_O_ID')
      end
    end
  end)

  c_id = client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if (not no_o_id[d_id]) then
        order_key[d_id] = 'NO_KEY';
      else
        order_key[d_id] = self.safeKey({w_id, d_id, no_o_id[1]});
      end
      p:hget('ORDERS.' .. order_key[d_id], 'O_C_ID')
    end
  end)

  ol_ids = client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if ((not no_o_id[d_id])) or ((not c_id[d_id])) then
        si_key = 'NO_KEY';
      else
        si_key = self.safeKey({no_o_id[d_id], d_id, w_id});
      end
      p:smembers('ORDER_LINE.INDEXES.SUMOLAMOUNT.' .. si_key)
    end
  end)

  pipe_results = client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if ((not no_o_id[d_id])) or ((not c_id[d_id])) then
        p:get('NULL_VALUE')
      else
        for _, i in ipairs(ol_ids[d_id]) do
          p:hget('ORDER_LINE.' .. tostring(i), 'OL_AMOUNT')
          ol_counts[d_id] = ol_counts[d_id] + 1
        end
      end
    end
  end)

  index = 0;
  counter = 0;
  for _, ol_amount in ipairs(pipe_results) do
    counter = counter + 1
    if counter  >  ol_counts[index + 1] then
      index = index + 1
      counter = 0;
    else
      if ol_amount then
        ol_total[index + 1] = ol_total[index + 1] + tonumber(ol_amount)
      end
    end
  end

  client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if ((not no_o_id[d_id])) or ((not c_id[d_id])) then
        if true then break end
      end
      no_key = self.safeKey({d_id, w_id, no_o_id[d_id]});
      no_si_key = self.safeKey({d_id, w_id});
      p:del('NEW_ORDER.' .. no_key)
      p:srem('NEW_ORDER.IDS', no_key)
      p:srem('NEW_ORDER.INDEXES.GETNEWORDER.' .. no_si_key, no_key)
      p:hset('ORDERS.' .. order_key[d_id], 'W_CARRIER_ID', o_carrier_id)
      for _, i in ipairs(ol_ids[d_id]) do
        p:hset('ORDER_LINE.' .. tostring(i), 'OL_DELIVERY_D', ol_delivery_d)
        ol_counts[d_id] = ol_counts[d_id] + 1
      end
    end
  end)

  old_balance = client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if ((not no_o_id[d_id])) or ((not c_id[d_id])) then
        p:get('NULL_VALUE')
        customer_key[d_id] = 'NO_KEY';
      else
        customer_key[d_id] = self.safeKey({w_id, d_id, c_id[d_id]});
        p:hget('CUSTOMER.' .. customer_key[d_id], 'C_BALANCE')
      end
    end
  end)

  client:pipeline(function(p)
    for d_id=1, 10 + 1 - 1, 1 do
      if ((not no_o_id[d_id])) or ((not c_id[d_id])) then
        if true then break end
      else
        new_balance = tonumber(old_balance[d_id]) + tonumber(ol_total[d_id]);
        p:hset('CUSTOMER.' .. customer_key[d_id], 'C_BALANCE', new_balance)
        table.insert(result, {d_id, no_o_id[d_id]})
      end
    end
  end)

  return result
end

local generateDeliveryParams = function()
  w_id = math.random(1, 4)
  o_carrier_id = math.random(1, 10)
  ol_delivery_d = os.time()

  return {w_id=w_id, o_carrier_id= o_carrier_id, ol_delivery_d= ol_delivery_d}
end

require 'chronos'
local start = chronos.nanotime()

for i=1,10000 do
    tpcc(generateDeliveryParams())
end

local stop = chronos.nanotime()
print(("Completed in %s seconds"):format(stop - start))
