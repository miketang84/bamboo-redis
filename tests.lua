require 'lglib'
require 'telescope'
local redis = require 'bamboo-redis'


local socket = require 'socket'


context("Bamboo Redis Testing", function ()
	local db
	before(function()
        db = redis.connect()
    end)

    after(function()
        db:quit()
    end)

	context("Basic commands", function ()
		test("set & del & exists", function ()
			db:set('test_001', 'xyz')
			assert_equal(db:exists('test_001'), true)
			local ctype = db:type('test_001')
			assert_equal(ctype, 'string')

			db:del('test_001')
			assert_equal(db:exists('test_001'), false)

		end)

		test("keys", function ()
			local ret = db:keys('*')
			assert_equal(type(ret) == 'table', true)
			--ptable(ret)
		end)
		
        test("incr & decr", function ()
            db:set('test_incr', 10)
			db:incr('test_incr')

			assert_equal(db:get('test_incr'), '11')
            db:decr('test_incr')
			assert_equal(db:get('test_incr'), '10')

		end)


		test("hget", function ()
			assert_equal(db:hget('______nnn___', 'nnn'), nil)
		end)

		test("lrange", function ()
			local ret = db:lrange('______nnn___', 0, -1)

			assert_equal(type(ret), 'table')
			assert_equal(#ret, 0)
		end)

		test("zadd, zrange, zscore ", function ()
			db:del('test_zset')
			db:zadd('test_zset', 10, 'xxx')
			db:zadd('test_zset', 20, 'yyy')
			db:zadd('test_zset', 30, 'zzz')
			db:zadd('test_zset', 40, 'www')

			local ret = db:zrange('test_zset', 0, -1)
			ptable(ret)
			assert_equal(type(ret), 'table')
			assert_equal(#ret, 4)
			assert_equal(ret[1], 'xxx')
			assert_equal(ret[2], 'yyy')
			assert_equal(ret[3], 'zzz')
			assert_equal(ret[4], 'www')

			assert_equal(db:zscore('test_zset', 'zzz'), 30)
			assert_equal(db:zscore('test_zset', 'zzzff'), nil)


			local ret, scores = db:zrange('test_zset', 0, -1, 'withscores')
			assert_equal(type(ret), 'table')
			assert_equal(#ret, 4)
			assert_equal(type(scores), 'table')
			assert_equal(#ret, 4)
			assert_equal(scores[1], 10)
			assert_equal(scores[2], 20)
			assert_equal(scores[3], 30)
			assert_equal(scores[4], 40)

			local ret, scores = db:zrevrange('test_zset', 0, -1, 'withscores')
			assert_equal(type(ret), 'table')
			assert_equal(#ret, 4)
			assert_equal(type(scores), 'table')
			assert_equal(#ret, 4)
			assert_equal(scores[4], 10)
			assert_equal(scores[3], 20)
			assert_equal(scores[2], 30)
			assert_equal(scores[1], 40)





		end)

		test("del, hmset & hmget", function ()
			db:del('test_hash')
			db:hmset('test_hash', 'field_1', '111', 'field_2', '222', 'field_3', '333', 'field_4', '444')
			local ret = db:hmget('test_hash', 'field_1', 'field_2', 'field_3', 'field_4')

			assert_equal(type(ret), 'table')
			assert_equal(#ret, 4)
			ptable(ret)

		end)

		test("pipeline", function ()
			db:del('test_hash')
			db:del('test_hash1')
			db:hmset('test_hash', 'field_1', '111', 'field_2', '222', 'field_3', '333', 'field_4', '444')
			db:hmset('test_hash1', 'field_1', 'aa', 'field_2', 'bb', 'field_3', 'cc', 'field_4', 'dd')

			local ret = db:pipeline(function (p)
				p:hgetall('test_hash')
				p:hgetall('test_hash1')
			end)

			fptable(ret)

--			assert_equal(type(ret), 'table')
--			assert_equal(#ret, 4)

		end)

		test("transaction - no watch", function ()
			db:del('test_hash')
			db:del('test_hash1')

			local ret = db:transaction(function (t)
				t:hmset('test_hash', 'field_1', '111', 'field_2', '222', 'field_3', '333', 'field_4', '444')
				t:hmset('test_hash1', 'field_1', 'aa', 'field_2', 'bb', 'field_3', 'cc', 'field_4', 'dd')
				local tmp = t:get('test_hash0')
				if type(tmp) ~= 'string' then tmp = nil end
				t:multi()
				t:hgetall('test_hash')
				if tmp then
					t:hgetall('test_hash1')
				end
				t:exec()
			end)

			print('---------- transaction - no watch ----------')
			fptable(ret)

--			assert_equal(type(ret), 'table')
--			assert_equal(#ret, 4)

		end)

		test("transaction - watch", function ()
			db:del('test_hash')
			db:del('test_hash1')

			db:hmset('test_hash', 'field_1', '111', 'field_2', '222', 'field_3', '333', 'field_4', '444')
			db:hmset('test_hash1', 'field_1', 'aa', 'field_2', 'bb', 'field_3', 'cc', 'field_4', 'dd')

			local options = { watch = { 'test_hash', 'test_hash1' }, retry=2 }
			local ret = db:transaction(function (t)
				local tmp = t:get('test_hash0')
				if type(tmp) ~= 'string' then tmp = nil end
				t:multi()
				t:hset('test_hash', 'nnn', 'n00')
				t:hset('test_hash1', 'mmm', 'm00')

				t:hgetall('test_hash')
				if tmp then
					t:hgetall('test_hash1')
				end
				t:hgetall('test_hash1')
				t:exec()
			end, options)

			print('---------- transaction - watch ----------')
			fptable(ret)

--			assert_equal(type(ret), 'table')
--			assert_equal(#ret, 4)

		end)


		test("hset, hdel & hgetall performance", function ()
			local t1 = socket.gettime()
			for i = 1, 1 do

			db:hset('test_hash', 'field_1', '111')
			db:hset('test_hash', 'field_2', '222')
			db:hset('test_hash', 'field_3', '333')
			db:hset('test_hash', 'field_4', '444')
			db:hset('test_hash', 'field_5', '555')
			db:hset('test_hash', 'field_6', '666')
			db:hset('test_hash', 'field_7', '777')
			db:hset('test_hash', 'field_8', '888')
			db:hset('test_hash', 'field_9', '999')
			db:hset('test_hash', 'field_0', '000')

			local ret = db:hgetall('test_hash')
			assert_equal(type(ret) == 'table', true)

			db:hdel('test_hash', 'field_1')
			db:hdel('test_hash', 'field_2')
			db:hdel('test_hash', 'field_3')
			db:hdel('test_hash', 'field_4')
			db:hdel('test_hash', 'field_5')
			db:hdel('test_hash', 'field_6')
			db:hdel('test_hash', 'field_7')
			db:hdel('test_hash', 'field_8')
			db:hdel('test_hash', 'field_9')
			db:hdel('test_hash', 'field_0')
			end
			local t2 = socket.gettime()

			print('time eclipse...', t2 - t1)
		end)

	end)
end)
