from pymongo import MongoClient
import datetime
from pymongo import DESCENDING

def insert_into_lineitems(db):
	db_lineitems = db.get_collection('lineitems')

	if not db_lineitems:
		db_lineitems = db.create_collection('lineitems')
		db_lineitems.create_index("shipdate")
		db_lineitems.create_index("order.customer.mktsegment")
		db_lineitems.create_index("order.customer.regionname")

	lineitem1 = {
		"_id": 1,
		"returnflag": "1",
		"linestatus": "1",
		"quantity": 1.7,
		"extendedprice": 1.7,
		"discount": 1.7,
		"tax": 1.7,
		"shipdate": datetime.datetime(2020, 12, 15),
		"suppnationkey": 1,
		"order":
			{
				"orderkey": 1,
				"orderdate": datetime.datetime(2020, 12, 15),
				"shippriority": 23,
				"customer":
					{
						"mktsegment": "segment",
						"nationkey": 1,
						"nationname":"Spain",
						"regionname": "EU"
					}
			}
	}
	lineitem2 = {
		"_id": 2,
		"returnflag": "1",
		"linestatus": "1",
		"quantity": 10.7,
		"extendedprice": 10.7,
		"discount": 10.7,
		"tax": 10.7,
		"shipdate": datetime.datetime(2020, 12, 20),
		"suppnationkey": 2,
		"order":
			{
				"orderkey": 1,
				"orderdate": datetime.datetime(2020, 12, 15),
				"shippriority": 23,
				"customer":
					{
						"mktsegment": "segment",
						"nationkey": 1,
						"nationname":"Spain",
						"regionname": "EU"
					}
			}
	}
	lineitem3 = {
		"_id": 3,
		"returnflag": "1",
		"linestatus": "1",
		"quantity": 13.7,
		"extendedprice": 13.7,
		"discount": 13.7,
		"tax": 13.7,
		"shipdate": datetime.datetime(2020, 12, 24),
		"suppnationkey": 1,
		"order":
			{
				"orderkey": 2,
				"orderdate": datetime.datetime(2020, 12, 22),
				"shippriority": 42,
				"customer":
					{
						"mktsegment": "segment2",
						"nationkey": 2,
						"nationname":"Francia",
						"regionname": "EU"
					}
			}
	}
	lineitem4 = {
		"_id": 4,
		"returnflag": "1",
		"linestatus": "1",
		"quantity": 21.7,
		"extendedprice": 21.7,
		"discount": 21.7,
		"tax": 21.7,
		"shipdate": datetime.datetime(2020, 12, 31),
		"suppnationkey": 1,
		"order":
			{
				"orderkey": 2,
				"orderdate": datetime.datetime(2020, 12, 22),
				"shippriority": 42,
				"customer":
					{
						"mktsegment": "segment2",
						"nationkey": 2,
						"nationname":"Francia",
						"regionname": "EU"
					}
			}
	}

	db_lineitems.insert_many([lineitem1, lineitem2, lineitem3, lineitem4])

def insert_into_suppliers(db):
	db_suppliers = db.get_collection('suppliers')
	if not db_suppliers:
		db_suppliers = db.create_collection('suppliers')
		db_suppliers.create_index("nation.region")

	supplier1 = {
		"_id": 1,
		"acctbal": 1.7,
		"name": "Amazon",
		"address": "adress1",
		"phone": "665346778",
		"comment": "comment",
		"nation":
			{
				"name": "Spain",
				"region": "EU"
			},
		"partsupp": [
			{
				"supplycost": 1.7,
				"part":
					{
						"partkey": 1,
						"mfgr": "mfgr",
						"size": 1,
						"type": "type"
					}
			},
			{
				"supplycost": 5.7,
				"part":
					{
						"partkey": 2,
						"mfgr": "mfgr2",
						"size": 2,
						"type": "type2"
					}
			}
		]
	}

	supplier2 = {
		"_id": 2,
		"acctbal": 14.7,
		"name": "name2",
		"address": "adress2",
		"phone": "632567880",
		"comment": "comments",
		"nation":
			{
				"name": "Francia",
				"region": "EU"
			},
		"partsupp": [
		]
	}

	supplier3 = {
		"_id": 3,
		"acctbal": 1.7,
		"name": "name3",
		"address": "adress3",
		"phone": "654321678",
		"comment": "comment234",
		"nation":
			{
				"name": "Spain",
				"region": "EU"
			},
		"partsupp": [
			{
				"supplycost": 14.7,
				"part":
					{
						"partkey": 1,
						"mfgr": "mfgr",
						"size": 1,
						"type": "type"
					}
			}
		]
	}

	supplier4 = {
		"_id": 4,
		"acctbal": 1.7,
		"name": "name4",
		"address": "adress4",
		"phone": "665346778",
		"comment": "comment656",
		"nation":
			{
				"name": "Spain",
				"region": "EU"
			},
		"partsupp": [
			{
				"supplycost": 16.7,
				"part":
					{
						"partkey": 3,
						"mfgr": "mfgr",
						"size": 10,
						"type": "type"
					}
			}
		]
	}

	db_suppliers.insert_many([supplier1, supplier2, supplier3, supplier4])

def query_1(db, date):
	result = db.get_collection('lineitems').aggregate(
		[
			{"$match": {"shipdate": {"$lte": date}}},
			{"$group":
				{
					"_id": None,
					"returnflag": {"$first": "$returnflag"},
					"linestatus": {"$first": "$linestatus"},
					"sum_qty": {"$sum": "$quantity"},
					"sum_base_price": {"$sum": "$extendedprice"},
					"sum_disc_price": {"$sum": {"$multiply": ["$extendedprice", {"$subtract": [1, "$discount"]}]}},
					"sum_charge": {"$sum": {"$multiply": ["$extendedprice", {"$multiply": [{"$subtract": [1, "$discount"]}, {"$add": [1, "$tax"]}]}]}},
					"avg_qty": {"$avg": "$quantity"},
					"avg_price": {"$avg": "$extendedprice"},
					"avg_disc": {"$avg": "$discount"},
					"count_order": {"$sum": 1}
				}
			},
			{"$sort": {"returnflag": 1, "linestatus": 1}},
			{"$project": { "_id": 0}}
		]
	)
	for document in result:
		print(document)

def query_2(db, size, type, region):
	segon_result = db.get_collection('suppliers').aggregate(
		[
			{"$match": {"nation.region": {"$eq": region}}},
			{"$unwind": "$partsupp"},
			{"$group":
				{
					"_id": None,
					"min_supplycost": {"$min": "$partsupp.supplycost"}
				}
			},
			{"$project": { "_id": 0, "min_supplycost": 1}}
		]
	)

	min_supplycost = segon_result.next()['min_supplycost']

	result = db.get_collection('suppliers').aggregate(
		[
			{"$unwind": "$partsupp"},
			{"$match":
				{"$and":
					[
						{"partsupp.part.size": {"$eq": size}},
						{"$and":
							[
								{"partsupp.part.type": {"$regex": type}},
								{"partsupp.supplycost": {"$eq": min_supplycost}}
							]
						}
					]
				}
			},
			{"$project":
				{
					"_id": 0,
					"acctbal": 1,
					"name": 1,
					"nation.name": 1,
					"partsupp.part.partkey": 1,
					"partsupp.part.mfgr": 1,
					"address": 1,
					"phone": 1,
					"comment": 1
				}
			}
		]
	)

	for document in result:
		print(document)

def query_3(db, segment, date1, date2):
	result = db.get_collection('lineitems').aggregate(
		[
			{"$match": {
				"order.customer.mktsegment": {"$eq": segment},
				"order.orderdate": {"$lt": date1},
				"shipdate": {"$gt": date2}
			}},
			{"$group":
				{
					"_id": None,
					"orderkey": {"$first": "$order.orderkey"},
					"orderdate": {"$first": "$order.orderdate"},
					"shippriority": {"$first": "$order.shippriority"},
					"revenue": { "$sum": {"$multiply": [{"$subtract": [1, "$discount"]}, "$extendedprice"]}},
				}
			},
			{"$sort": {
				"revenue": DESCENDING, "orderdate": 1
			}},
			{"$project": { "_id": 0}}
		]
	)
	for document in result:
		print(document)

def query_4(db, region, date):
	date2 = date.replace(date.year + 1)

	result = db.get_collection('lineitems').aggregate(
		[
			{"$match": {
				"$expr":{"$eq":["$suppnationkey", "$order.customer.nationkey"]},
				"order.customer.regionname": {"$eq": region},
				"order.orderdate": {"$gte": date, "$lt": date2}
			}},
			{"$group":
				{
					"_id": None,
					"name": {"$first": "$order.customer.nationname"},
					"revenue": {"$sum": {"$multiply": [{"$subtract": [1, "$discount"]}, "$extendedprice"]}},
				}
			},
			{"$sort": {
				"revenue": DESCENDING
			}},
			{"$project": { "_id": 0}}
		]
	)

	for document in result:
		print(document)

if __name__ == "__main__":
	client = MongoClient('localhost', 27017)
	db = client.test

	insert_into_lineitems(db)
	insert_into_suppliers(db)

	print("Query 1:")
	date = input("Introduce una date con el formato año-mes-dia (Ejemplo: 2020-12-15): ")
	date = datetime.datetime.strptime(date, "%Y-%m-%d")
	query_1(db, date)

	print("Query 2:")
	size = int(input("Introduce un size (Ejemplo: 1): "))
	type = input("Introduce un type (Ejemplo: type): ")
	region = input("Introduce una region (Ejemplo: EU): ")
	query_2(db, size, type, region)

	print("Query 3:")
	segment = input("Introduce un segment (Ejemplo: segment): ")
	date1 = input("Introduce una date con el formato año-mes-dia (Ejemplo: 2021-12-15): ")
	date1 = datetime.datetime.strptime(date1, "%Y-%m-%d")
	date2 = input("Introduce una date con el formato año-mes-dia (Ejemplo: 2019-12-15): ")
	date2 = datetime.datetime.strptime(date2, "%Y-%m-%d")
	query_3(db, segment, date1, date2)

	print("Query 4:")
	region = input("Introduce una region (Ejemplo: EU): ")
	date = input("Introduce una date con el formato año-mes-dia (Ejemplo: 2020-12-15): ")
	date = datetime.datetime.strptime(date, "%Y-%m-%d")
	query_4(db, region, date)

	db_lineitems = db.get_collection('lineitems')
	db_lineitems.drop()

	db_suppliers = db.get_collection('suppliers')
	db_suppliers.drop()


