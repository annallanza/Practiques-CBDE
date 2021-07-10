from neo4j import GraphDatabase
from datetime import datetime

def delete_all(db):
	db.session().run("MATCH (n)"
					 "DETACH DELETE n")

def create_Lineitem(db, orderkey, supplierkey, returnflag, linestatus, quantity, extendedprice, discount, tax, shipdate):
	result = db.session().run("CREATE (l:LineItem { l_orderkey:$orderkey, supplier_key:$supplierkey, l_returnflag:$returnflag, l_linestatus:$linestatus, l_quantity:$quantity, l_extendedprice:$extendedprice, l_discount:$discount, l_tax:$tax, l_shipdate:$shipdate})"
							  "RETURN l", orderkey=orderkey, supplierkey=supplierkey, returnflag=returnflag, linestatus=linestatus, quantity=quantity, extendedprice=extendedprice, discount=discount, tax=tax, shipdate=shipdate)

	for record in result:
		print(record)

def create_Order(db, orderkey, orderdate, shippriority, mktsegment, nationkey, n_name, regionkey, r_name):
	result = db.session().run("CREATE (o:Order {o_orderkey:$orderkey, o_orderdate:$orderdate, o_shippriority:$shippriority, c_mktsegment:$mktsegment, c_n_nationkey:$nationkey, c_n_name:$n_name, c_n_r_regionkey:$regionkey, c_n_r_name:$r_name})" 
							  "RETURN o", orderkey=orderkey, orderdate=orderdate, shippriority=shippriority, mktsegment=mktsegment, nationkey=nationkey, n_name=n_name, regionkey=regionkey, r_name=r_name)
	
	for record in result:
		print(record)
	
def create_PartSupplier(db, partkey, suppkey, supplycost):
	result = db.session().run("CREATE (ps:PartSupplier {ps_partkey:$partkey, ps_suppkey:$suppkey, ps_supplycost:$supplycost})" 
							  "RETURN ps", partkey=partkey, suppkey=suppkey, supplycost=supplycost)

	for record in result:
		print(record)
	
def create_Part(db, parkey, mfgr, size, type):
	result = db.session().run("CREATE (p:Part {p_partkey:$parkey, p_mfgr:$mfgr, p_size:$size, p_type:$type})" 
							  "RETURN p", parkey=parkey, mfgr=mfgr, size=size, type=type)
	for record in result:
		print(record)

def create_Supplier(db, suppkey, acctbal, name, address, phone, comment, nationkey, n_name, regionkey, r_name):
	result = db.session().run("CREATE (s:Supplier {s_suppkey:$suppkey, s_acctbal:$acctbal, s_name:$name, s_address:$address, s_phone:$phone, s_comment:$comment, n_nationkey:$nationkey, n_name:$n_name, n_r_regionkey:$regionkey, n_r_name:$r_name}) " 
							  "RETURN s", suppkey=suppkey, acctbal=acctbal, name=name, address=address, phone=phone, comment=comment, nationkey=nationkey, n_name=n_name, regionkey=regionkey, r_name=r_name)

	for record in result:
		print(record)

def create_relationship_HasLineItems(db, orderkey):
	db.session().run("MATCH (o:Order {o_orderkey:$orderkey}),(l:LineItem {l_orderkey:$orderkey})"
					 "CREATE (o)-[:HasLineItems]->(l)", orderkey=orderkey)

def create_relationship_HasPartSuppliers(db, orderkey, parkey):
	db.session().run("MATCH (l:LineItem {l_orderkey:$orderkey}),(ps:PartSupplier {ps_partkey:$parkey})"
					 "CREATE (l)-[:HasPartSuppliers]->(ps)", orderkey=orderkey, parkey=parkey)

def create_relationship_HasParts(db, partkey):
	db.session().run("MATCH (ps:PartSupplier {ps_partkey:$partkey}),(p:Part {p_partkey:$partkey})"
					 "CREATE (ps)-[:HasParts]->(p)", partkey=partkey)

def create_relationship_HasSuppliers(db, partkey, suppkey):
	db.session().run("MATCH (ps:PartSupplier {ps_partkey:$partkey}),(s:Supplier {s_suppkey:$suppkey})"
					 "CREATE (ps)-[:HasSuppliers]->(s)", partkey=partkey, suppkey=suppkey)

def options():
	print("Elige una de estas opciones:")
	print("1.Query 1")
	print("2.Query 2")
	print("3.Query 3")
	print("4.Query 4")
	print("5.Inserts")
	print("6.Salir")

def inserts(db):
	delete_all(db)

	create_Lineitem(db, 1, 1, 'RF1', 'lineStatus1', 23, 2.3, 4.5, 20, datetime(2020, 10, 2).strftime('%Y-%m-%d'))
	create_Lineitem(db, 1, 2, 'RF2', 'lineStatus2', 54, 6.3, 8.5, 23, datetime(2021, 10, 2).strftime('%Y-%m-%d'))
	create_Lineitem(db, 1, 1, 'RF1', 'lineStatus1', 23, 3.3, 4.5, 27, datetime(2022, 10, 2).strftime('%Y-%m-%d'))

	create_Lineitem(db, 2, 2, 'RF1', 'lineStatus1', 2, 1.3, 1.5, 10, datetime(2020, 10, 2).strftime('%Y-%m-%d'))
	create_Lineitem(db, 2, 3, 'RF2', 'lineStatus2', 5, 1.3, 8.5, 13, datetime(2021, 10, 2).strftime('%Y-%m-%d'))
	create_Lineitem(db, 2, 2, 'RF1', 'lineStatus1', 2, 23.3, 4.5, 17, datetime(2022, 10, 2).strftime('%Y-%m-%d'))

	create_Lineitem(db, 3, 3, 'RF1', 'lineStatus1', 123, 2.3, 4.5, 24, datetime(2020, 10, 2).strftime('%Y-%m-%d'))
	create_Lineitem(db, 3, 1, 'RF2', 'lineStatus2', 154, 6.3, 1.5, 27, datetime(2021, 10, 2).strftime('%Y-%m-%d'))
	create_Lineitem(db, 3, 3, 'RF1', 'lineStatus1', 123, 3.3, 1.5, 21, datetime(2022, 10, 2).strftime('%Y-%m-%d'))
	print('-' * 50)
	create_Order(db, 1, datetime(2020, 10, 2).strftime('%Y-%m-%d'), 1, 'MK1', 1, 'nationName1', 1, 'regionName1')
	create_Order(db, 2, datetime(2021, 10, 2).strftime('%Y-%m-%d'), 1, 'MK2', 1, 'nationName1', 2, 'regionName2')
	create_Order(db, 3, datetime(2022, 10, 2).strftime('%Y-%m-%d'), 1, 'MK2', 123, 'nationName123', 123, 'regionName123')
	print('-' * 50)
	create_PartSupplier(db, 1, 1, 0.5)
	create_PartSupplier(db, 2, 2, 6.7)
	create_PartSupplier(db, 3, 3, 16.7)
	print('-' * 50)
	create_Part(db, 1, 'mfgr1', 1, 'type1')
	create_Part(db, 2, 'mfgr2', 65, 'type2')
	create_Part(db, 3, 'mfgr3', 165, 'type3')
	print('-' * 50)
	create_Supplier(db, 1, 0.87, 'supplier1', 'adress1', '555', 'comment1', 1, 'nationName1', 1, 'regionName1')
	create_Supplier(db, 2, 6.87, 'supplier2', 'adress2', '555', 'comment2', 1, 'nationName1', 2, 'regionName2')
	create_Supplier(db, 3, 16.87, 'supplier3', 'adress3', '555', 'comment3', 123, 'nationName123', 123, 'regionName123')
	print('-' * 50)
	create_relationship_HasLineItems(db, 1)
	create_relationship_HasLineItems(db, 2)
	create_relationship_HasLineItems(db, 3)
	create_relationship_HasPartSuppliers(db, 1, 1)
	create_relationship_HasPartSuppliers(db, 2, 2)
	create_relationship_HasPartSuppliers(db, 3, 3)
	create_relationship_HasParts(db, 1)
	create_relationship_HasParts(db, 2)
	create_relationship_HasParts(db, 3)
	create_relationship_HasSuppliers(db, 1, 1)
	create_relationship_HasSuppliers(db, 2, 2)
	create_relationship_HasSuppliers(db, 3, 3)

	indexes(db)

def indexes(db):
	db.session().run("DROP INDEX mktsegment_index IF EXISTS")
	db.session().run("DROP INDEX orderdate_index IF EXISTS")
	db.session().run("DROP INDEX shipdate_index IF EXISTS")

	db.session().run("CREATE INDEX mktsegment_index FOR (o:Order) ON (o.c_mktsegment)")
	db.session().run("CREATE INDEX orderdate_index FOR (o:Order) ON (o.o_orderdate)")
	db.session().run("CREATE INDEX shipdate_index FOR (l:LineItem) ON (l.l_shipdate)")

def query_1(db, shipdate):
	result = db.session().run("MATCH(l:LineItem) "
							  "WHERE l.l_shipdate <= $shipdate " 
							  "WITH l.l_returnflag as returnflag, l.l_linestatus as linestatus, sum(l.l_quantity) as sum_qty, "
							  "sum(l.l_extendedprice) as sum_base_price, sum(l.l_extendedprice*(1-l.l_discount)) as sum_disc_price, " 
							  "sum(l.l_extendedprice*(1-l.l_discount)*(1+l.l_tax)) as sum_charge, avg(l.l_quantity) as avg_qty, " 
							  "avg(l.l_extendedprice) as avg_price, avg(l.l_discount) as avg_disc, count(*) as count_order "
							  "RETURN returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order "
							  "ORDER BY returnflag, linestatus ", shipdate=shipdate)

	for record in result:
		print(record)

def query_2(db, size, type, r_name):
	result = db.session().run("MATCH (ps:PartSupplier)-[:HasSuppliers]->(s:Supplier) " 
							  "WHERE s.n_r_name = $r_name "
							  "WITH min(ps.ps_supplycost) As minsupplycost " 
							  "RETURN minsupplycost", r_name=r_name)

	for record in result:
		supplycost = record['minsupplycost']

	result = db.session().run("MATCH (ps:PartSupplier)-[:HasSuppliers]->(s:Supplier), (ps)-[:HasParts]->(p:Part) "
							  "WHERE s.n_r_name = $r_name AND p.p_size = $size AND p.p_type ENDS WITH $type AND ps.ps_supplycost = $supplycost "
 							  "RETURN  s.s_acctbal, s.s_name, s.n_name, p.p_partkey, p.p_mfgr, s.s_address, s.s_phone, s.s_comment "
							  "ORDER BY s.s_acctbal desc, s.n_name, s.s_name, p.p_partkey", size=size, type=type, r_name=r_name, supplycost=supplycost)

	for record in result:
		print(record)

def query_3(db, c_segment, orderdate, shipdate):
	result = db.session().run("MATCH(o:Order)-[:HasLineItems]->(l:LineItem) "
							  "WHERE o.c_mktsegment = $c_segment AND o.o_orderdate < $orderdate AND l.l_shipdate > $shipdate "
							  "WITH l.l_orderkey as orderkey, sum(l.l_extendedprice*(1-l.l_discount)) as revenue, o.o_orderdate as orderdate, o.o_shippriority as shippriority "
							  "RETURN orderkey, revenue, orderdate, shippriority "
							  "ORDER BY revenue DESC, orderdate", c_segment=c_segment, orderdate=orderdate, shipdate=shipdate)

	for record in result:
		print(record)

def query_4(db, orderdate, r_name):
	orderdate = datetime.strptime(orderdate, '%Y-%m-%d')
	orderdate2 = orderdate.replace(year=(orderdate.year + 1))
	orderdate = orderdate.strftime('%Y-%m-%d')
	orderdate2 = orderdate2.strftime('%Y-%m-%d')

	result = db.session().run("MATCH(o:Order)-[:HasLineItems]->(l:LineItem), (l)-[:HasPartSuppliers]->(ps:PartSupplier),(ps)-[:HasSuppliers]->(s:Supplier) "
							  "WHERE o.c_n_name = s.n_name AND s.n_r_name = $r_name AND o.o_orderdate >= $orderdate AND o.o_orderdate < $orderdate2 "
							  "WITH s.n_name as n_name, sum(l.l_extendedprice*(1-l.l_discount)) as revenue "
							  "RETURN n_name, revenue "
							  "ORDER BY revenue DESC", orderdate=orderdate, orderdate2=orderdate2, r_name=r_name)

	for record in result:
		print(record)

if __name__ == '__main__':
	uri = "neo4j://localhost:7687"
	db = GraphDatabase.driver(uri, auth=("neo4j", "Anna"))

	options()
	opcion = int(input())

	while opcion != 6:
		print('-' * 50)
		if opcion == 1:
			print("QUERY 1:")
			print("Escribe una fecha siguiendo el formato año-mes-dia (2020-10-02):")
			fecha = input()
			query_1(db, fecha)
		elif opcion == 2:
			print("QUERY 2:")
			print("Escribe el tipo (type1):")
			tipo = input()
			print("Escribe el nombre de la region (regionName1):")
			region_name = input()
			query_2(db, 1, tipo, region_name)
		elif opcion == 3:
			print("QUERY 3:")
			print("Escribe el mktsegment (MK1):")
			segment = input()
			print("Escribe la fecha de orderdate siguiendo el formato año-mes-dia (2022-10-02):")
			fecha = input()
			print("Escribe la fecha de shipdate siguiendo el formato año-mes-dia (2020-10-02):")
			fecha2 = input()
			query_3(db, segment, fecha, fecha2)
		elif opcion == 4:
			print("QUERY 4:")
			print("Escribe la fecha de orderdate siguiendo el formato año-mes-dia (2020-08-02):")
			fecha = input()
			print("Escribe el nombre de la region (regionName1):")
			region_name = input()
			query_4(db, fecha, region_name)
		elif opcion == 5:
			inserts(db)

		print('-' * 50)
		options()
		opcion = int(input())
	
	db.close()