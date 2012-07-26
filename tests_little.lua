require 'lglib'
require 'telescope'
local redis = require 'bamboo-redis'
local db = redis.connect()

local socket = require 'socket'


context("Bamboo Redis Testing", function ()
	context("Basic commands", function ()
		test("set & del & exists", function ()
			db:set('test_001', 'xyz')
			assert_equal(db:exists('test_001'), true)
			db:del('test_001')
			assert_equal(db:exists('test_001'), false)
		end)

		test("keys", function ()
			local ret = db:keys('*')
			assert_equal(type(ret) == 'table', true)
			ptable(ret)
		end)
		
		test("hset, hdel & hgetall", function ()
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
