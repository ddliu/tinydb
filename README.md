# TinyDB

Light weight database layer on top of PDO

## Features

 - Based on PDO, so common databases are supported
 - <del>Switch between different connections(might be useful for replication)</del>
 - Query builder
 - Simple ORM based on Table or Model
 
## Usage

### Setup

	require('path/to/tinydb.php');
	$db = new TinyDB('mysql:host=localhost;dbname=testdb', 'username', 'password');

### Query Builder

	//create query builder
	$cmd = $db->command();

	//select
	$user = $cmd->select('username, email')->from('user')->where('uid=:uid', array(':uid' => 1))->queryRow();

	//insert
	$cmd->reset()->insert('user', array(
		'username' => 'user1',
		'email' => 'email1',
	));

	//update
	$cmd->reset()->update('user', array(
		'email' => 'new email'
	), 'uid=:uid', array(':uid' => 1));

	//delete
	$cmd->reset()->delete('user', 'uid=:uid', array(':uid' => 1));

### Model Factory
	
	//You can use table name with a "@" prefix as a model
	$factory = $db->factory('@user');
	
	//Or use a model class
	$factory = $db->factory('User');
	
	//Get all users
	$users = $factory->findAll();
	foreach($users as $user){
		echo $user->email;
	}
	
	//find(findByPK)
	$user = $factory->find(1);
	
	//findManyByXX 
	$users = $factory->findManyByAge(30);
	
	//findOneByXX
	$user = $factory->findOneByUsername('user1');
	
	//create a model
	$user = $factory->create();
	
	//and then set data
	$user->username = 'user2';
	$user->email = 'email2';
	
	//You can also create this model with initial data
	$user = $factory->create(array(
		'username' => 'user2',
		'email' => 'email2',
	));
	
	//save model
	$user->save();
	
	//delete model
	$user->delete();
	
## License

Licensed under the [MIT license](http://www.opensource.org/licenses/mit-license.php)