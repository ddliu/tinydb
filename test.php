<?php
require dirname(__FILE__).'/tinydb.php';

class TinyDBTest extends PHPUnit_Framework_TestCase{
	protected $db;
	protected function setup(){
		$this->db = new TinyDB('sqlite::memory:');
		$this->db->exec("
				CREATE TABLE IF NOT EXISTS contact (
					id INTEGER PRIMARY KEY, 
					name TEXT, 
					email TEXT 
				)
			 ");
	}
	
	public function testConnection(){
		//show table
		$query = $this->db->query("SELECT * FROM sqlite_master WHERE type='table'");
		$row = $query->fetch(PDO::FETCH_ASSOC);
		$this->assertEquals($row['name'], 'contact');
		
		//insert
		$rst = $this->db->exec("INSERT INTO contact (name, email) VALUES ('test', 'test@test.com')");
		$this->assertEquals($rst, 1);
		
		//count
		$query = $this->db->query("SELECT COUNT(*) FROM contact");
		$this->assertEquals($query->fetchColumn(), 1);
	}
	
	public function testCommand(){
		$cmd = $this->db->command();
		//insert
		$rst = $cmd->insert('contact', array(
			'name' => 'test',
			'email' => 'test@test.com',
		));
		$this->assertEquals($rst, 1);
		
		$total = $cmd->reset()->select('COUNT(*)')->from('contact')->queryScalar();
		$this->assertEquals($total, 1);
		
		//select
		$name = $cmd->reset()->select('name')->from('contact')->limit(1)->queryScalar();
		$this->assertEquals($name, 'test');
		
		//update
		$rst = $cmd->reset()->update('contact', 
			array(
				'email' => 'newemail@test.com',
			),
			'name=:name',
			array(
				':name' => 'test',
			)
		);
		
		$this->assertEquals($rst, 1);
		
		//select
		$email = $cmd->reset()->select('email')->from('contact')->limit(1)->queryScalar();
		$this->assertEquals($email, 'newemail@test.com');
		
		//delete
		$rst = $cmd->reset()->delete('contact', 'name=:name', array(
			':name' => 'test',
		));
		
		$this->assertEquals($rst, 1);
		
		//count
		$rst = $cmd->reset()->select('COUNT(*)')->from('contact')->queryScalar();
		$this->assertEquals($rst, 0);
	}
	
	public function testModel(){
		$factory = $this->db->factory('@contact');
		
		//insert
		$model = $factory->create();
		$model->name = 'test';
		$model->email = 'test@test.com';
		$rst = $model->save();
		$this->assertEquals($rst, 1);
		
		$fetchModel = $factory->find(1);
		$this->assertEquals($fetchModel->name, 'test');
		
		//update
		$model->email = 'newemail@test.com';
		$rst = $model->save();
		$this->assertEquals($rst, 1);
		
		$fetchModel = $factory->findOneByName('test');
		$this->assertEquals($fetchModel->email, 'newemail@test.com');
		
		//delete
		$rst = $model->delete();
		$this->assertEquals($rst, 1);
		
		$count = $factory->count();
		$this->assertEquals($count, 0);
	}
	
	public function testTransaction(){
	}
}