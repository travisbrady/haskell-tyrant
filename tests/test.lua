function fibonacci(key, value)
   local num = tonumber(key)
   local large = math.pow((1 + math.sqrt(5)) / 2, num)
   local small = math.pow((1 - math.sqrt(5)) / 2, num)
   return (large - small) / math.sqrt(5)
end

function hi(key, value)
    return "hello, world!"
end
