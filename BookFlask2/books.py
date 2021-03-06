from flask import Flask, render_template, redirect, request
from cassandra.query import dict_factory
from cassandra.cluster import Cluster
import uuid

app = Flask(__name__, static_url_path = "")

cluster = Cluster()
session = cluster.connect('keyspace1')
# session.row_factory = dict_factory

table_name = "time_exp4"
driver_table_name = "time_exp5"

session.execute("CREATE TABLE IF NOT EXISTS %s(id uuid, property text, value text, primary key(id, property));"%table_name)

session.execute("CREATE INDEX IF NOT EXISTS on %s(value);"%table_name)

#Normally, id would be of type uuid, but with only one driver currently it is text
session.execute("CREATE TABLE IF NOT EXISTS %s(id text, name text, car text, phone text, rate text, rides list<uuid>, primary key(id));"%driver_table_name)

#Add first (and so far, only) driver
driver_id = 'id'
session.execute("INSERT INTO "+driver_table_name+"(id, name, car, phone, rate) values('id', 'driver1', 'car1', '800-800-8001', '20')")

#Homepage
@app.route('/')
def splash():
	return app.send_static_file('splash.html')


#The page to add a book to the database
@app.route('/add/', methods=['GET', 'POST'])
def add():
	if request.method == 'POST':
		new_data = {k : v for k, v in request.form.items()}
		#If the user leaves a field blank
		if new_data['name'] == '' or new_data['phone'] == '' or new_data['pickup'] == '' or new_data['destination'] == '' or new_data['time'] == '':
			return render_template('add.html', alert="required")
		else:
			#Check to see if a ride with same name and time exists in database
			select_results_name = session.execute("SELECT * FROM "+table_name+" WHERE property = 'name' AND value = %s", (new_data['name'],))
			select_results_time = session.execute("SELECT * FROM "+table_name+" WHERE property = 'time' AND value = %s", (new_data['time'],))
			for name in select_results_name:
				for time in select_results_time:
					if name.id == time.id:		
						return render_template('add.html', alert = "exists")

			#wrap in transaction?
			id = uuid.uuid4()
			insert_statement = "INSERT INTO "+table_name+"(id, property, value) values("+str(id)+", %s, %s)"
			session.execute(insert_statement, ('name', new_data['name']))	
			session.execute(insert_statement, ('phone', new_data['phone']))	
			session.execute(insert_statement, ('time', new_data['time']))	
			session.execute(insert_statement, ('pickup', new_data['pickup']))	
			session.execute(insert_statement, ('destination', new_data['destination']))			
			return render_template('add.html', alert = "success")
	else:
		return render_template('add.html', alert="")


#The search page
@app.route('/search/', methods=['GET', 'POST'])
def search():
	#Return results for titles, authors and genres that match the search query
	if request.method == 'POST':
		query = request.form['query']
		
		id_select_statement = "SELECT id FROM "+table_name+" WHERE property = %s and value = %s ALLOW FILTERING"

		user_name_ids = session.execute(id_select_statement, ('name', query))

		dest_ids = session.execute(id_select_statement, ('destination', query))

		pickup_ids = session.execute(id_select_statement, ('pickup', query))

		phone_ids = session.execute(id_select_statement, ('phone', query))

		driver_name_ids = session.execute(id_select_statement, ('driver_name', query))

		value_select_statement = "SELECT value FROM "+table_name+" WHERE id = %s and property = %s LIMIT 1 ALLOW FILTERING"

		# Creates dict of attributes for certain id
		results_dict = {}
		for row in user_name_ids:
			id = row.id
			user_name = session.execute(value_select_statement, (id, 'name'))[0]
			# dest = session.execute(value_select_statement, (id, 'destination'))[0]
			# pickup = session.execute(value_select_statement, (id, 'pickup'))[0]
			inner_dict = {'name': user_name.value, }#'pickup': pickup.value, 'destination': dest.value}perty
			results_dict[str(id)] = inner_dict

		for row in pickup_ids:
			id = row.id
			user_name = session.execute(value_select_statement, (id, 'name'))[0]
			# dest = session.execute(value_select_statement, (id, 'destination'))[0]
			# pickup = session.execute(value_select_statement, (id, 'pickup'))[0]
			inner_dict = {'name': user_name.value, }#'pickup': pickup.value, 'destination': dest.value}perty
			results_dict[str(id)] = inner_dict

		for row in dest_ids:
			id = row.id
			user_name = session.execute(value_select_statement, (id, 'name'))[0]
			# dest = session.execute(value_select_statement, (id, 'destination'))[0]
			# pickup = session.execute(value_select_statement, (id, 'pickup'))[0]
			inner_dict = {'name': user_name.value, }#'pickup': pickup.value, 'destination': dest.value}perty
			results_dict[str(id)] = inner_dict


		for row in phone_ids:
			id = row.id
			user_name = session.execute(value_select_statement, (id, 'name'))[0]
			# dest = session.execute(value_select_statement, (id, 'destination'))[0]
			# pickup = session.execute(value_select_statement, (id, 'pickup'))[0]
			inner_dict = {'name': user_name.value, }#'pickup': pickup.value, 'destination': dest.value}perty
			results_dict[str(id)] = inner_dict


		for row in driver_name_ids:
			id = row.id
			user_name = session.execute(value_select_statement, (id, 'name'))[0]
			# dest = session.execute(value_select_statement, (id, 'destination'))[0]
			# pickup = session.execute(value_select_statement, (id, 'pickup'))[0]
			inner_dict = {'name': user_name.value, }#'pickup': pickup.value, 'destination': dest.value}perty
			results_dict[str(id)] = inner_dict
		print(results_dict)

		return render_template('search_cass.html', posting=True, query=query, results=results_dict)#, dest_results=pickup_dict)

	else:
		return render_template('search_cass.html', posting=False)


#Individual information page for each book
@app.route('/detail/<id>/', methods=['GET', 'POST'])
def detail(id):
	id = uuid.UUID(id)
	result = {}
	all_properties = session.execute("SELECT property, value FROM "+table_name+" WHERE id = %s", (id,)) 
	for property in all_properties:
		result[property.property] = property.value
	return render_template('detail_cass.html', result=result, id=id)


@app.route('/unclaimed_rides/', methods=['GET', 'POST'])
def unclaimed_rides():
	if request.method == 'GET':
		# results = rides.find({'claimed':False})
		select_results = session.execute("SELECT id, property, value FROM "+table_name)

		# Create dictionary of dictionary containing attributes related by id
		results = {}
		for attribute in select_results:			
			unclaimed_select_results = session.execute("SELECT id FROM " + table_name + " WHERE id = %s and property = %s", (attribute.id, 'driver_name'))
			if len(unclaimed_select_results) == 0 :
				if attribute.id in results :
					results[attribute.id][attribute.property] = attribute.value
				else :
					results[attribute.id] = {}

		# unclaimed_results = {}
		# # Create new dict with only dicts that have no driver_name property
		# for row in results :
		# 	unclaimed_select_results = session.execute("SELECT id FROM " + table_name + " WHERE id = %s and property = %s ALLOW FILTERING", (row, 'driver_name'))
		# 	if len(unclaimed_select_results) == 0 :
		# 		unclaimed_result[row.key()] = row.value()
		# 	# if 'driver_name' in row :
		# 	# 	unclaimed_results[row.key()] = row.value()

		return render_template('unclaimed_rides.html', posting=False, result=results)



@app.route('/claim/<id>/', methods=['GET', 'POST'])
def claim(id):	
	already_claimed = True
	id = uuid.UUID(id)
	# Claimed if has driver_name attribute
	if request.method == 'POST' :
		#Wrap up in batch: now atomic, but how to make isolated? Only row-level currently
		session.execute("BEGIN BATCH \
			INSERT INTO "+table_name+"(id, property, value) values(%s, 'driver_id', %s); \
			INSERT INTO "+table_name+"(id, property, value) values(%s, 'driver_name', %s); \
			UPDATE "+driver_table_name+" SET rides = rides + [%s] WHERE id = %s; \
			APPLY BATCH;", (id, str(driver_id), id, 'placeholder', id, str(driver_id)))

		# for key, value in request.form.iteritems() :
		# 	# #Only worried about new fields
		# 	session.execute("INSERT INTO "+table_name+"(id, property, value) values("+str(id)+", %s, %s)", (key, value))
		# 	# session.execute("UPDATE " + table_name + " SET value = %s WHERE id = %s AND property = %s", (value, id, key))

	unclaimed_select_results = session.execute("SELECT id FROM " + table_name + " WHERE id = %s and property = %s", (id, 'driver_name'))
	if len(unclaimed_select_results) == 0 :
		already_claimed = False

	result = {}
	all_properties = session.execute("SELECT property, value FROM "+table_name+" WHERE id = %s", (id,)) 
	for property in all_properties:
		result[property.property] = property.value
	return render_template('claim.html', result = result, id = id, already_claimed=already_claimed)


@app.route('/drivers/', methods=['GET', 'POST'])
def drivers(): 
	select_results = session.execute("SELECT * FROM "+driver_table_name)
	results = {}
	for row in select_results:
		results[str(row.id)] = {'name': row.name, 'car': row.car, 'phone':row.phone, 'rate':row.rate, 'rides':row.rides}

	# ride_names = {}
	# ride_select_results = session.execute("SELECT id, value FROM "+table_name+" WHERE property = 'name' ALLOW FILTERING")
	# for row in ride_select_results:
	# 	ride_names[str(row.id)] = row.value

	return render_template('drivers.html', result = results)#, ride_names = ride_names)


if __name__ == '__main__':
	app.debug = True
	app.run()




