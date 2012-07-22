<?php

class TinyDB
{
	protected $currentConnectionName;
	protected $connections;
	public function __construct($dsn = null, $username = null, $password = null){
	}
	
	public function getPDO(){}
	
	public function addConnection(){}
	
	public function switchConnection(){}

	public function factory(){}
	
	public function command(){}
	
	public function query(){}
	
	public function exec(){}
	
	public function beginTransaction(){}
	
	public function rollBack(){}
	
	public function commit(){}
	
	public function inTransaction(){}
	
	public function lastInsertId(){}
	
	public function quote(){}
	
	public function quoteTable(){}
	
	public function quoteColumn(){}
	
	public function quoteIdentifier(){}
}

class TinyDBModel
{
}

class TinyDBFactory
{
}

class TinyDBCommand
{
	public function __construct(){
	}
	
	public function select(){}
	
	public function from(){}
	
	public function where(){}
	
	public function orderBy(){}
	
	public function limit(){}
	
	public function offset(){}
	
	public function join(){}
	
	public function leftJoin(){}
	
	public function innerJoin(){}
	
	public function groupBy(){}
	
	public function having(){}
	
	public function findAll(){}
	
	public function findOne(){}
}
