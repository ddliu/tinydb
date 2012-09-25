<?php
class TinyDBException extends Exception{}

/**
 * Connection management/PDO Wrapper
 */
class TinyDB
{
	protected $current;
	protected $connections = array();
	protected $configs = array();
	public function __construct($dsn = null, $username = null, $password = null, $options = array()){
		$this->current = 'default';
		$this->addConnection($this->current, $dsn, $username, $password, $options);
	}
	
	public function getConfig($name = null){
		if(null === $name){
			$name = $this->current;
		}
		return $this->configs[$name];
	}
	
	public function getPDO($name = null){
		if(null === $name){
			$name = $this->current;
		}
		if(!isset($this->connections[$name])){
			$config = $this->configs[$name];
			$this->connections[$name] = new PDO($config['dsn'], $config['username'], $config['password'], $config['options']);
		}
		
		return $this->connections[$this->current];
	}
	
	/**
	 * Add a new connection configuration
	 * 
	 * @param string $name
	 * @param string $dsn
	 * @param string $username
	 * @param string $password
	 * @param array $options
	 */
	public function addConnection($name, $dsn, $username = null, $password = null, $options = array()){
		//default driver options
		static $defaultOptions = array(
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			//PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
		);
		$this->configs[$name] = array(
			'dsn' => $dsn,
			'username' => $username,
			'password' => $password,
			'options' => $options,
		);
		
		return $this;
	}
	
	/**
	 * Set current connection
	 * @param string $name
	 */
	public function switchConnection($name = 'default'){
		$this->current = $name;
		
		return $this;
	}

	/**
	 * Create a model factory
	 * 
	 * @param string $model
	 * @return TinyDBFactory
	 */
	public function factory($model){
		return new TinyDBFactory($this, $model);
	}
	
	/**
	 * Create command
	 * 
	 * @return TinyDBCommand
	 */
	public function command(){
		return new TinyDBCommand($this);
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.getattribute.php
	 */
	public function getAttribute($attribute){
		return $this->getPDO()->getAttribute($attribute);
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.setattribute.php
	 */
	public function setAttribute($attribute, $value){
		return $this->getPDO()->setAttribute($attribute, $value);
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.prepare.php
	 */
	public function prepare(){
		return call_user_func_array(array($this->getPDO(), 'prepare'), func_get_args());
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.query.php
	 */
	public function query(){
		return call_user_func_array(array($this->getPDO(), 'query'), func_get_args());
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.exec.php
	 */
	public function exec(){
		return call_user_func_array(array($this->getPDO(), 'exec'), func_get_args());
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.begintransaction.php
	 */
	public function beginTransaction(){
		return $this->getPDO()->beginTransaction();
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.rollback.php
	 */
	public function rollBack(){
		return $this->getPDO()->rollBack();
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.commit.php
	 */
	public function commit(){
		return $this->getPDO()->commit();
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.intransaction.php
	 */
	public function inTransaction(){
		return $this->getPDO()->inTransaction();
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.lastinsertid.php
	 */
	public function lastInsertId(){
		return call_user_func_array(array($this->getPDO(), 'lastInsertId'), func_get_args());
	}
	
	/**
	 * @link http://www.php.net/manual/en/pdo.quote.php
	 */
	public function quote(){
		return call_user_func_array(array($this->getPDO(), 'quote'), func_get_args());
	}
	
	/**
	 * Quote table name
	 * @param string $name
	 * @return string;
	 */
	public function quoteTable($name){
		return $this->quoteIdentifier($name);
	}
	
	/**
	 * Quote column name
	 * @param string $name
	 * @return string;
	 */
	public function quoteColumn($name){
		return $this->quoteIdentifier($name);
	}
	
	/**
	 * Quote table or column
	 * @param string $name
	 * @return string
	 */
	public function quoteIdentifier($name){
		$quote = null;
		switch($this->getAttribute(PDO::ATTR_DRIVER_NAME)){
			case 'pgsql':
			case 'sqlsrv':
			case 'dblib':
			case 'mssql':
			case 'sybase':
				$quote = '"';
			case 'mysql':
			case 'sqlite':
			case 'sqlite2':
			default:
				$quote = '`';
		}
		
		$parts = explode('.', $name);
		foreach($parts as $k => $part){
			if($part !== '*'){
				$parts[$k] = $quote.$part.$quote;
			}
		}
		
		return implode('.', $parts);
	}
	
	public function buildLimitOffset($sql, $limit, $offset = 0){
		switch($this->getAttribute(PDO::ATTR_DRIVER_NAME)){
			case 'sqlsrv':
			case 'dblib':
			case 'mssql':
			case 'sybase':
				throw new TinyDBException('Limit/offset not implemented yet');
			case 'pgsql':
			case 'mysql':
			case 'sqlite':
			case 'sqlite2':
			default:
				if($limit > 0){
					$sql .= "\n LIMIT ".$limit;
				}
				if($offset > 0){
					$sql .= " OFFSET ".$offset;
				}
				
				return $sql;
		}
	}
}

/**
 * Model
 */
class TinyDBModel
{
	### Model defination ###
	
	private $_table;
	
	private $_pk;
	
	private $_relations;
	
	### end ###
	
	protected $_db;
	
	protected $_data = array();
	
	protected $_safe;
	
	protected $_dirty = array();
	
	protected $_isNew;
	
	/**
	 * Model configuration
	 * 
	 * Sub classes should extend this static method
	 * @return array
	 */
	public static function config(){
		return array(
			//'table' => 'table',
			'pk' => 'id',
			//'relations' => array(),
		);
	}
	
	/**
	 * Get model configuration
	 * 
	 * @param string $key
	 * @return mixed
	 */
	final public static function getConfig($key = null){
		$config = static::config();
		if(null === $key){
			return $config;
		}
		elseif(isset($config[$key])){
			return $config[$key];
		}
		else{
			return null;
		}
	}
	
	/**
	 * Get model table
	 */
	public function getTable(){
		return $this->_table;
	}

	/**
	 * Get model pk
	 */
	public function getPK(){
		return $this->_pk;
	}
		
	/**
	 * Get model relations
	 */
	public function getRelations(){
		return $this->_relations;
	}
	
	/**
	 * Translate class name to table name.
	 * example:
	 *  BlogPost => blog_post
	 *  Acme\BlogPost => blog_post
	 * @param string $class Class name
	 * @return string
	 */
	static public function entityNameToDBName($class){
		//namespace
		if(false !== $pos = strrpos($class, '\\')){
			$class = substr($class, $pos + 1);
		}
		
		return strtolower(preg_replace('/(?<=[a-z])([A-Z])/', '_$1', $class));
	}
	
	/**
	 * Model constructor
	 * 
	 * @param TinyDB $db
	 * @param array $data
	 * @boolean $isNew
	 */
	public function __construct($db, $data = array(), $isNew = true){
		$this->_db = $db;
		
		if($isNew){
			$this->_dirty = $data;
		}
		else{
			$this->_data = $data;
		}
		$this->_isNew = $isNew;
		
		//this requires PHP 5.3
		$this->setAttributes(static::config());
	}
	
	/**
	 * Set attribute
	 */
	public function setAttribute($attribute, $value){
		if($attribute === 'table'){
			$this->_table = $value;
		}
		elseif($attribute === 'pk'){
			$this->_pk = $value;
		}
		elseif($attribute === 'relations'){
			$this->_relations = $value;
		}
		
		return $this;
	}
	
	/**
	 * Set attributes(table, pk...)
	 * @param array $attributes
	 */
	public function setAttributes($attributes){
		foreach($attributes as $k => $v){
			$this->setAttribute($k, $v);
		}
		
		return $this;
	}
	
	public function isNew(){
		return $this->_isNew;
	}
	
	public function isDirty(){
		return !empty($this->_dirty);
	}
	
	/**
	 * Get raw data
	 * @param string $key
	 * @return mixed
	 */
	public function getRaw($key = null){
		if(null === $key){
			return $this->_data;
		}
		else{
			return isset($this->_data[$key])?$this->_data[$key]:null;
		}
	}
	
	/**
	 * Get data
	 * @param string $key
	 * @return mixed Find in dirty data first, return all data if key is not specified
	 */
	public function get($key = null){
		if(null === $key){
			return array_merge($this->_data, $this->_dirty);
		}
		else{
			if(isset($this->_dirty[$key])){
				return $this->_dirty[$key];
			}
			elseif(isset($this->_data[$key])){
				return $this->_data[$key];
			}
			else{
				return null;
			}
		}
	}
	
	public function __get($key){
		$relations = $this->getRelations();
		if(isset($relations[$key])){
			return $this->getWithRelation($key);
		}
		return $this->get($key);
	}
	
	public function getWithRelation($name){
		$relations = $this->getRelations();
		if(isset($relations[$name])){
			$relation = $relations[$name];
			if($relation['relation'] === 'OTO'){
				return $this->getOneToOne($relation['target'], isset($relation['key'])?$relation['key']:null, isset($relation['target_key'])?$relation['target_key']:null);
			}
			elseif($relation['relation'] == 'OTM'){
				return $this->getOneToMany($relation['target'], isset($relation['key'])?$relation['key']:null, isset($relation['target_key'])?$relation['target_key']:null);
			}
			elseif($relation['relation'] == 'MTO'){
				return $this->getManyToOne($relation['target'], isset($relation['key'])?$relation['key']:null, isset($relation['target_key'])?$relation['target_key']:null);
			}
			elseif($relation['relation'] == 'MTM'){
				return $this->getManyToMany($relation['target'], $relation['through'], isset($relation['key'])?$relation['key']:null, isset($relation['target_key'])?$relation['target_key']:null);
			}
			else{
				throw new TinyDBException('Invalid relation "'.$relation['relation'].'"');
			}
		}
		else{
			return false;
		}
	}
	
	/**
	 * Has one
	 * 
	 * @param string $target target model or @table
	 * @param string $key
	 * @param string $target_key
	 * 
	 * @return TinyDBModel
	 */
	public function getOneToOne($target, $key = null, $target_key = null){
		$factory = $this->_db->factory($target);
		if(null === $key){
			$key = $this->getPK();
		}
		if(null === $target_key){
			$target_key = $factory->getPK();
		}
		
		return $factory->findOneBy($target_key, $this->get($key));
	}
	
	/**
	 * Has many
	 * 
	 * @param string $target
	 * @param string $key
	 * @param string $target_key
	 * @return array
	 */
	public function getOneToMany($target, $key = null, $target_key = null){
		$factory = $this->_db->factory($target);
		if(null === $key){
			$key = $this->getPK();
		}
		if(null === $target_key){
			$target_key = $key;
		}
		
		return $factory->findManyBy($target_key, $this->get($key));
	}
	
	/**
	 * Belongs to
	 * @param string $target
	 * @param string $key
	 * @param string $target_key
	 * 
	 * @return TinyDBModel
	 */
	public function getManyToOne($target, $key = null, $target_key = null){
		$factory = $this->_db->factory($target);
		if(null === $target_key){
			$target_key = $factory->getPK();
		}
		if(null === $key){
			$key = $target_key;
		}
		
		return $factory->findOneBy($target_key, $this->get($key));
	}
	
	/**
	 * Many to many
	 * 
	 * @param string $target
	 * @param string $through
	 * @param string $key
	 * @param string $target_key
	 * @return array
	 */
	public function getManyToMany($target, $through, $key = null, $target_key = null){
		$factory = $this->_db->factory($target);

		if(null === $key){
			$key = $this->getPK();
		}
		if(null === $target_key){
			$target_key = $factory->getPK();
		}
		
		$through = $this->parseThrough($through);
		if(!$through[1]){
			$through[1] = $key;
		}
		if(!$through[2]){
			$through[2] = $target_key;
		}
		
		$rows = $this->_db->command()
			->select('t.*')
			->from($factory->getTable().' t')
			->leftJoin($through[0].' m', 'm.'.$through[2].'=t.'.$target_key)
			->where('m.'.$through[1].'=:value', array(
				':value' => $this->get($key)
			))
		->queryAll();
		
		if(false === $rows){
			return false;
		}
		
		return $factory->mapModels($rows);
	}
	
	protected function parseThrough($through){
		$through = explode(',', $through);
		$table = trim($through[0]);
		$key = isset($through[1])?trim($through[1]):null;
		$target_key = isset($through[2])?trim($through[2]):null;
		
		return array($table, $key, $target_key);
	}
	
	/**
	 * Set data
	 * @param string|array $key
	 * @param mixed $value
	 */
	public function set($key, $value = null){
		if(is_array($key)){
			$this->_dirty = $key;
		}
		else{
			$this->_dirty[$key] = $value;
		}
		
		return $this;
	}
	
	public function __set($key, $value){
		$this->_dirty[$key] = $value;
	}
	
	public function __isset($key){
		return isset($this->_dirty[$key]) || isset($this->_data[$key]);
	}
	
	public function __unset($key){
		if(isset($this->_dirty[$key])){
			unset($this->_dirty[$key]);
		}
	}
	
	protected function buildPKConditions(){
		$pk = $this->getPK();
		if(is_string($pk)){
			$pks = array($pk);
		}
		else{
			$pks = $pk;
		}
		$params = array();
		foreach($pks as $k => $pk){
			$pks[$k] = $pk.'=:pk'.$k;
			$params[':pk'.$k] = $this->_data[$pk];
		}
		array_unshift($pks, 'AND');
		
		return array($pks, $params);
	}
	
	/**
	 * Save modified data to db
	 * @return int|boolean
	 */
	public function save(){
		if($this->beforeSave()){
			if($this->isNew()){
				$data = $this->_dirty;
				
				//insert
				if(false !== $rst = $this->_db->command()->insert($this->getTable(), $data))
				{
					if(is_string($this->getPK()) && $id = $this->_db->lastInsertId()){
						$data[$this->getPK()] = $id;
					}
					$this->_data = $data;
					$this->_dirty = array();
					$this->_isNew = false;
					$this->afterSave();
					return $rst;
				}
			}
			else{
				if($this->isDirty()){
					//update
					$pkConditions = $this->buildPKConditions();
					if(false !== $rst = $this->_db->command()->update($this->getTable(), $this->_dirty, $pkConditions[0], $pkConditions[1])){
						$this->_data = array_merge($this->_data, $this->_dirty);
						$this->_dirty = array();
						$this->afterSave();
						return $rst;
					}
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Delete model
	 * @return int|boolean
	 */
	public function delete(){
		if($this->beforeDelete()){
			$pkConditions = $this->buildPKConditions();
			if(false !== $rst = ($this->isNew() || $this->_db->command()->delete($this->getTable(), $pkConditions[0], $pkConditions[1]))){
				$this->_data = array();
				$this->_dirty = array();
				$this->afterDelete();
				
				return $rst;
			}
		}
		
		return false;
	}
	
	protected function beforeSave(){
		return true;
	}
	
	protected function afterSave(){
		return true;
	}
	
	protected function beforeDelete(){
		return true;
	}
	
	protected function afterDelete(){
		return true;
	}
	
}

/**
 * Model factory
 */
class TinyDBFactory
{
	protected $db;
	protected $modelClass;
	protected $table;
	protected $pk;
	protected $with;
	
	public function __construct($db, $model, $pk = null){
		$this->db = $db;
		if($model[0] === '@'){
			$this->modelClass = 'TinyDBModel';
			$this->table = substr($model, 1);
		}
		else{
			$this->modelClass = $model;
			if(null !== $model::getConfig('table')){
				$this->table = $model::getConfig('table');
			}
			else{
				$this->table = $model::entityNameToDBName($model);
			}
		}
		if(null !== $pk){
			$this->pk = $pk;
		}
		else{
			$class = $this->modelClass;
			$this->pk = $class::getConfig('pk');
		}
	}
	
	/**
	 * PK getter
	 */
	public function getPK(){
		return $this->pk;
	}
	
	/**
	 * Table getter
	 */
	public function getTable(){
		return $this->table;
	}
	
	/**
	 * @todo fetch related data together
	 */
	public function with($with){
		$this->with = $with;
		
		return $this;
	}
	
	public function buildWith(){
		$with = $this->with;
		$columns = explode(',', $with);
		foreach($columns as $column){
			if(strpos($column, '.')
		}
		$this->with = null;
	}
	
	/**
	 * Map array to model
	 * 
	 * @param array $row
	 * @return TinyDBModel
	 */
	public function map($row){
		$class = $this->modelClass;
		$model = new $class($this->db, $row, false);
		$model->setAttribute('table', $this->table)->setAttribute('pk', $this->pk);
		return $model;
	}
	
	/**
	 * Map data rows to model array
	 * 
	 * @param array $rows
	 * @return array
	 */
	public function mapModels($rows){
		$rst = array();
		foreach($rows as $row){
			$rst[] = $this->map($row);
		}
		
		return $rst;
	}
	
	/**
	 * Create a fresh model from array
	 * 
	 * @param array $row
	 * @param TinyDBModel
	 */
	public function create($row = array()){
		$class = $this->modelClass;
		$model = new $class($this->db, $row);
		$model->setAttribute('table', $this->table)->setAttribute('pk', $this->pk);
		
		return $model;
	}
	
	/**
	 * Build PK condition(multiple pk is also supported)
	 * @param string|array $pk
	 * @param mixed $_data
	 * @return array
	 */
	protected function buildPKConditions($pk, $_data){
		if(is_string($pk)){
			$pks = array($pk);
			if(!is_array($_data)){
				$_data = array($pk => $_data);
			}
		}
		else{
			$pks = $pk;
		}
		$params = array();
		foreach($pks as $k => $pk){
			$pks[$k] = $pk.'=:pk'.$k;
			$params[':pk'.$k] = $_data[$pk];
		}
		array_unshift($pks, 'AND');
		
		return array($pks, $params);
	}
	
	/**
	 * Count with conditions
	 * 
	 * @param string|array $conditions
	 * @param array $params
	 * @return integer
	 */
	public function count($conditions = '', $params = array()){
		return $this->db->command()->select('COUNT(*)')->from($this->table)->where($conditions, $params)->queryScalar();
	}
	
	/**
	 * Count by key
	 * 
	 * @param string $key
	 * @param mixed $value
	 * @return integer
	 */
	public function countBy($key, $value){
		return $this->count($key.'=:key', array(':key' => $value));
	}
	
	/**
	 * Find by PK
	 * @param int $id
	 */
	public function find($pk){
		return $this->findByPK($pk);
	}
	
	/**
	 * Find all
	 */
	public function findAll(){
		$rows = $this->db->command()->select()->from($this->table)->queryAll();
		if(false === $rows){
			return $rows;
		}
		
		return $this->mapModels($rows);
	}
	
	/**
	 * Find one model with conditions
	 * 
	 * @param string|array $conditions
	 * @param array $params
	 * @return boolean|TinyDBModel
	 */
	public function findOne($conditions, $params = array()){
		$row = $this->db->command()->select()->from($this->table)->where($conditions, $params)->limit(1)->queryRow();
		if(false === $row){
			return false;
		}
		
		return $this->map($row);
	}
		
	/**
	 * Find one model by key
	 * 
	 * @param string $key
	 * @param mixed $value
	 * @return boolean|TinyDBModel
	 */
	public function findOneBy($key, $value){
		return $this->findOne($key.'=:key', array(':key' => $value));
	}
	
	/**
	 * Find one model by primary key
	 * 
	 * @param string|array $pk
	 * @return boolean|TinyDBModel
	 */
	public function findByPK($pk){
		$pkConditions = $this->buildPKConditions($this->pk, $pk);
		return $this->findOne($pkConditions[0], $pkConditions[1]);
	}
	
	/**
	 * Find many models with conditions
	 * 
	 * @param string|array $conditions
	 * @param array $params
	 * @param string|array $orderBy
	 * @param int $limit
	 * @param int $offset
	 * @return boolean|array
	 */
	public function findMany($conditions = '', $params = array(), $orderBy = null, $limit = null, $offset = null){
		$cmd = $this->db->command()->select()->from($this->table)->where($conditions, $params);
		if($orderBy){
			$cmd->orderBy($orderBy);
		}
		$rows = $cmd->limit($limit, $offset)->queryAll();
		if(false === $rows){
			return false;
		}
		else{
			return $this->mapModels($rows);
		}
	}
	
	/**
	 * Find many models with by key
	 * 
	 * @param string $key
	 * @param mixed $value
	 * @param string|array $orderBy
	 * @param int $limit
	 * @param int $offset
	 * @return boolean|array
	 */
	public function findManyBy($key, $value,  $orderBy = null, $limit = null, $offset = null){
		return $this->findMany($key.'=:key', array(':key' => $value), $orderBy, $limit, $offset);
	}
	
	/**
	 * Update table with condition
	 * 
	 * @param array $values
	 * @param string|array $conditions
	 * @param array $params
	 * @return int|boolean
	 */
	public function update($values, $conditions = '', $params = array()){
		return $this->db->command()->update($this->table, $values, $conditions, $params);
	}
	
	/**
	 * Update table by key
	 * 
	 * @param string $key
	 * @param mixed $value
	 * @param array $values
	 * @return int|boolean
	 */
	public function updateBy($key, $value, $values){
		return $this->update($values, $key.'=:key', array(':key' => $value));
	}
	
	/**
	 * Update table by primary key
	 * @param string|array $pk
	 * @param array $values
	 * @return int|boolean
	 */
	public function updateByPK($pk, $values){
		$pkConditions = $this->buildPKConditions($this->pk, $pk);
		return $this->update($values, $pkConditions[0], $pkConditions[1]);
	}
	
	/**
	 * Insert
	 * 
	 * @param array $values
	 * @return int|boolean
	 */
	public function insert($values){
		return $this->db->command()->insert($this->table, $values);
	}
	
	/**
	 * Delete with condition
	 * 
	 * @param string|array $conditioins
	 * @param array $params
	 * @return int|boolean
	 */
	public function delete($conditions = '', $params = array()){
		return $this->db->command()->delete($this->table, $conditions, $params);
	}
	
	/**
	 * Delete by key
	 * 
	 * @param string $key
	 * @param mixed $value
	 * @return int|boolean
	 */
	public function deleteBy($key, $value){
		return $this->delete($key.'=:key', array(':key' => $value));
	}
	
	/**
	 * Delete by primary key
	 * 
	 * @param string|array $pk
	 * @return int|boolean
	 */
	public function deleteByPK($pk){
		$pkConditions = $this->buildPKConditions($this->pk, $pk);
		return $this->delete($pkConditions[0], $pkConditions[1]);
	}
	
	/**
	 * Override __call magic method to provide many helper methods
	 * 
	 * @param string $name
	 * @param array $arguments
	 * @return mixed
	 */
	public function __call($name, $arguments){
		//findOneByXXX/findManyByXXX
		if(preg_match('#^find(One|Many)By(.+)$#', $name, $matches)){
			$one = $matches[1] === 'One';
			$findByKey = TinyDBModel::entityNameToDBName($matches[2]);
			array_unshift($arguments, $findByKey);
			if($one){
				return call_user_func_array(array($this, 'findOneBy'), $arguments);
			}
			else{
				return call_user_func_array(array($this, 'findManyBy'), $arguments);
			}
		}
		elseif(preg_match('#^(update|delete|count)By(.+)$#', $name, $matches)){
			$action = $matches[1];
			$actionByKey = TinyDBModel::entityNameToDBName($matches[2]);
			array_unshift($arguments, $actionByKey);
			return call_user_func_array(array($this, $action.'By'), $arguments); 
		}
		else{
			throw new TinyDBException(sprintf('Helper method "%s" does not exist', $name));
		}
	}
}

/**
 * Database command/query builder
 */
class TinyDBCommand
{
	protected $db;
	protected $statement;
	protected $params = array();
	protected $query = array();
	protected $sql;
	
	/**
	 * Constructor
	 * 
	 * @param TinyDB $db Database connection
	 */
	public function __construct($db){
		$this->db = $db;
	}
	
	protected function matchAlias($entry){
		if(preg_match('#^(.*?)(?i:\s+as\s+|\s+)(.*)$#', $entry, $matches)){
			return array(
				$matches[1],
				$matches[2],
			);
		}
		else{
			return false;
		}
	}
	
	protected function splitParts($parts){
		return preg_split('#^\s*,\s*#', trim($parts), -1, PREG_SPLIT_NO_EMPTY);
	}
	
	/**
	 * Reset command
	 */
	public function reset(){
		$this->statement = null;
		$this->params = array();
		$this->query = array();
		$this->sql = null;
		
		return $this;
	}
	
	/**
	 * Prepare PDO statement
	 */
	public function prepare(){
		if(null === $this->statement){
			$this->statement = $this->db->prepare($this->getSql());
		}
		
		return $this;
	}
	
	public function bindParam(){
		$this->prepare();
		call_user_func_array(array($this->statement, 'bindParam'), func_get_args);
	}
	
	public function bindValue(){
		$this->prepare();
		call_user_func_array(array($this->statement, 'bindValue'), func_get_args());
		
		return $this;
	}
	
	public function bindValues($values){
		$this->prepare();
		
		foreach($values as $k=> $value){
			$this->statement->bindValue($k, $value);
		}
		
		return $this;
	}
	
	public function mergeParams($params){
		foreach($params as $k => $v){
			$this->params[$k] = $v;
		}
	}
	
	/**
	 * SELECT statement
	 * example:
	 * 	select('contact.*, user.email)
	 *  select(array('contact.*', 'user.email'))
	 * @param string|array $fields
	 */
	public function select($fields = '*'){
		if(is_string($fields) && strpos($fields, '(') !== false){
			$this->query['select'] = $fields;
		}
		else{
			if(!is_array($fields)){
				$fields = $this->splitParts($fields);
			}
			foreach($fields as $k => $field){
				if(false === strpos($field, '(')){
					if($alias = $this->matchAlias($field)){
						$fields[$k] = $this->db->quoteColumn($alias[0]).' AS '.$this->db->quoteColumn($alias[1]);
					}
					else{
						$fields[$k] = $this->db->quoteColumn($field);
					}
				}
			}
			$this->query['select'] = implode(', ', $fields);
		}
		
		return $this;
	}
	
	/**
	 * DISTINCT statement
	 */
	public function distinct(){
		$this->query['distinct'] = true;
		
		return $this;
	}
	
	/**
	 * FROM statement
	 * example:
	 *  from('contact, user AS U')
	 *  from(array('contact', 'user AS U'))
	 * @param string|array $tables
	 */
	public function from($tables){
		if(is_string($tables)){
			$tables = $this->splitParts($tables);
		}
		foreach($tables as $k => $table){
			if($alias = $this->matchAlias($table)){
				$tables[$k] = $this->db->quoteTable($alias[0]).' AS '.$this->db->quoteTable($alias[1]);
			}
			else{
				$tables[$k] = $this->db->quoteTable($table);
			}
		}
		
		$this->query['from'] = implode(', ', $tables);
		
		return $this;
	}
	
	/**
	 * Build condition SQL
	 * @param string|array $conditions
	 */
	public function buildConditions($conditions){
		if(!is_array($conditions)){
			if(null === $conditions){
				return '';
			}
			return $conditions;
		}
		elseif($conditions === array()){
			return '';
		}
		
		$n = count($conditions);
		$operator = strtoupper($conditions[0]);
		if($operator === 'AND' || $operator === 'OR'){
			$result = array();
			for($i = 1; $i < $n; $i++){
				$condition = $this->buildConditions($conditions[$i]);
				if('' !== $condition){
					$result[] = '('.$condition.')';
				}
			}
			if($result === array()){
				return '';
			}
			else{
				return implode(' '.$operator.' ', $result);
			}
		}
		
		if(!isset($conditions[1], $conditions[2])){
			return '';
		}
		
		$column = $this->db->quoteColumn($conditions[1]);
		$values = $conditions[2];
		if(!is_array($values)){
			$values = array($values);
		}
		if($operator === 'IN' || $operator === 'NOT IN'){
			if($values === array()){
				return $operator === 'IN'?'0':'';
			}
			foreach($values as $k => $value){
				$values[$k] = $this->db->quote($value);
			}
			
			return $column.' '.$operator.' ('.implode(',', $values).')';
		}
		elseif($operator === 'LIKE' || $operator === 'NOT LIKE' || $operator === 'OR LIKE' || $operator === 'OR NOT LIKE'){
			if($values === array()){
				return ($operator === 'LIKE' || $operator === 'OR LIKE')?'0':'';
			}
			if($operator === 'LIKE' || $operator === 'NOT LIKE'){
				$andor = 'AND';
			}
			else{
				$andor = 'OR';
				$operator = $operator === 'OR LIKE'?'LIKE':'NOT LIKE';
			}
			
			$result = array();
			foreach($values as $k => $value){
				$result[] = $column.' '.$operator.' '.$this->db->quote($value);
			}
			
			return implode($andor, $result);
		}
		else{
			throw new TinyDBException('Invalid operator "'.$operator.'"');
		}
	}
	
	/**
	 * WHERE statement
	 * @param string|array $conditions
	 * @param array $params
	 */
	public function where($conditions, $params = array()){
		$this->mergeParams($params);
		$this->query['where'] = $this->buildConditions($conditions);
		
		return $this;
	}
	
	/**
	 * ORDER BY statement
	 * example:
	 *  orderBy('id DESC')
	 *  orderBy('firstname, lastname DESC')
	 * @param string|array $fields
	 */
	public function orderBy($fields){
		if(!is_array($fields)){
			$fields = $this->splitParts($fields);
		}
		
		foreach($fields as $k => $field){
			if(preg_match('#^(.*?)\s+(asc|desc)$#i', $field, $matches)){
				$fields[$k] = $this->db->quoteColumn($matches[1]).' '.$matches[2];
			}
			else{
				$fields[$k] = $this->db->quoteColumn($field);
			}
		}
		
		$this->query['order'] = implode(',', $fields);
		
		return $this;
	}
	
	/**
	 * LIMIT statement
	 * @param int $limit
	 * @param int $offset
	 */
	public function limit($limit, $offset = null){
		$this->query['limit'] = $limit;
		if(null !== $offset){
			$this->offset($offset);
		}
		
		return $this;
	}
	
	/**
	 * OFFSET statement
	 * @param int $offset
	 */
	public function offset($offset){
		$this->query['offset'] = $offset;
		
		return $this;
	}
	
	/**
	 * * JOIN statement
	 * @param string $type LEFT JOIN|RIGHT JOIN...
	 * @param string $table
	 * @param string|array $conditions
	 * @param array $params
	 */
	protected function anyJoin($type, $table, $conditions = '', $params = array()){
		$this->mergeParams($params);
		if($alias = $this->matchAlias($table)){
			$table = $this->db->quoteTable($alias[0]).' AS '.$this->db->quoteTable($alias[1]);
		}
		else{
			$table = $this->db->quoteTable($table);
		}
		$conditions = $this->buildConditions($conditions);
		if('' !== $conditions){
			$conditions = ' ON '.$conditions;
		}
		
		if(isset($this->query['join']) && is_string($this->query['join'])){
			$this->query['join'] = array($this->query['join']);
		}
		
		$this->query['join'][] = $type.' '.$table.' '.$conditions;
		
		return $this;
	}
	
	/**
	 * JOIN statement
	 * 
	 * @param string $table
	 * @param string|array $conditions
	 * @param array $params
	 */
	public function join($table, $conditions = '', $params = array()){
		return $this->anyJoin('JOIN', $table, $conditions, $params);
	}
	
	/**
	 * LEFT JOIN statement
	 * 
	 * @param string $table
	 * @param string|array $conditions
	 * @param array $params
	 */
	public function leftJoin($table, $conditions = '', $params = array()){
		return $this->anyJoin('LEFT JOIN', $table, $conditions, $params);
	}
	
	/**
	 * RIGHT JOIN statement
	 * 
	 * @param string $table
	 * @param string|array $conditions
	 * @param array $params
	 */
	public function rightJoin($table, $condtions = '', $params = array()){
		return $this->anyJoin('RIGHT JOIN', $table, $conditions, $params);
	}
	
	/**
	 * GROUP BY statement
	 * 
	 * @param string|array $fields
	 */
	public function groupBy($fields){
		if(!is_array($fields)){
			$fields = $this->splitParts($fields);
		}
		
		foreach($fields as $k => $field){
			$fields[$k] = $this->db->quoteColumn($field);
		}
		
		$this->query['group'] = implode(',', $fields);
		
		return $this;
	}
	
	/**
	 * HAVING statement
	 * 
	 * @param string|array $conditions
	 * @param array $params
	 */
	public function having($conditions, $params = array()){
		$this->mergeParams($params);
		$this->query['having'] = $this->buildConditions($conditions);
		
		return $this;
	}
	
	/**
	 * UNION statement
	 * 
	 * @param string $sql
	 */
	public function union($sql){
		if(isset($this->query['union']) && is_string($this->query['union'])){
			$this->query['union'][] = $this->query['union'];
		}
		
		$this->query['union'][] = $sql;
		
		return $this;
	}
	
	/**
	 * Build query SQL
	 * 
	 * @param array $query use $this->query if not specified
	 * @return string
	 */
	public function buildQuery($query = null){
		if(null === $query){
			$query = $this->query;
		}
		
		$sql = "SELECT ";
		if(isset($query['distinct']) && $query['distinct']){
			$sql .= 'DISTINCT ';
		}
		$sql .= isset($query['select'])?$query['select']:'*';
		
		if(!isset($query['from'])){
			return false;
		}
		
		$sql .= "\nFROM ".$query['from'];
		if(isset($query['join'])){
			$sql .= "\n".(is_array($query['join'])?implode("\n", $query['join']):$query['join']);
		}
		if(isset($query['where']) && $query['where'] !== ''){
			$sql .= "\nWHERE ".$query['where'];
		}
		if(isset($query['group'])){
			$sql .= "\nGROUP BY ".$query['group'];
			if(isset($query['having'])){
				$sql .= "\nHAVING ".$query['having'];
			}
		}
		if(isset($query['order'])){
			$sql .= "\n ORDER BY ".$query['order'];
		}
		$limit = isset($query['limit'])?$query['limit']:0;
		$offset = isset($query['offset'])?$query['offset']:0;
		$sql = $this->db->buildLimitOffset($sql, $limit, $offset);
		
		if(isset($query['union'])){
			$sql .= "\n".(is_array($query['union'])?implode("\n", $query['union']):$query['union']);
		}
		
		return $sql;
	}
	
	/**
	 * Set SQL for this command
	 * 
	 * @param string $sql
	 */
	public function setSql($sql){
		$this->sql = $sql;
		
		return $this;
	}
	
	/**
	 * Get SQL for this command
	 * 
	 * @return string
	 */
	public function getSql(){
		if(null === $this->sql){
			if(!empty($this->query)){
				$this->sql = $this->buildQuery();
			}
			else{
				return false;
			}
		}
		
		return $this->sql;
	}
	
	/**
	 * Prepare statement before query
	 * @param array $params
	 */
	protected function beginQuery($params = array()){
		$params = array_merge($this->params, $params);
		$this->prepare();
		$this->statement->execute($params);
	}
	
	/**
	 * Query
	 * Statement cursor should be closed after fetching data($statement->closeCursor)
	 * 
	 * @param array $params
	 * @return PDOStatement
	 */
	public function query($params = array()){
		$this->beginQuery($params);
		return $this->statement;
	}
	
	/**
	 * Get query result as array
	 * 
	 * @param array $params
	 */
	public function queryAll($params = array()){
		$this->beginQuery($params);
		$rst = $this->statement->fetchAll(PDO::FETCH_ASSOC);
		$this->statement->closeCursor();
		return $rst;
	}
	
	/**
	 * Get first row of result
	 * 
	 * @param array $params
	 */
	public function queryRow($params = array()){
		$this->beginQuery($params);
		$rst = $this->statement->fetch(PDO::FETCH_ASSOC);
		$this->statement->closeCursor();
		return $rst;
	}
	
	/**
	 * Get first column of result set
	 * 
	 * @param array $params
	 */
	public function queryColumn($params = array()){
		$this->beginQuery($params);
		$rst = $this->statement->fetchAll(PDO::FETCH_COLUMN);
		$this->statement->closeCursor();
		return $rst;
	}
	
	/**
	 * Get first column of first row of result set
	 * 
	 * @param array $params
	 */
	public function queryScalar($params = array()){
		$this->beginQuery($params);
		$rst = $this->statement->fetchColumn();
		$this->statement->closeCursor();
		
		return $rst;
	}
	
	/**
	 * Execute statement and return rows affected
	 * 
	 * @param array $params
	 */
	public function execute($params = array()){
		$this->prepare();
		if(false === $rst = $this->statement->execute($params)){
			return false;
		}
		
		return $this->statement->rowCount();
	}
	
	/**
	 * Insert data
	 * 
	 * @param string $table
	 * @param array $values
	 * @return int Rows affected, false on error
	 */
	public function insert($table, $values){
		$keys = array();
		$params = array();
		$placeholders = array();
		foreach($values as $k => $v){
			$keys[] = $this->db->quoteColumn($k);
			$params[':'.$k] = $v;
			$placeholders[] = ':'.$k;
		}
		$sql = "INSERT INTO ".$this->db->quoteTable($table).' ('.implode(', ',$keys).') VALUES ('.implode(',', $placeholders).')';
		return $this->setSql($sql)->execute($params);
	}
	
	/**
	 * Update table
	 * 
	 * @param string $table
	 * @param array $values
	 * @param string|array $conditions
	 * @param array $params
	 * @return int Rows affected, false on error
	 */
	public function update($table, $values, $conditions = '', $params = array()){
		$updates = array();
		foreach($values as $k => $value){
			$updates[] = $this->db->quoteColumn($k).' = :'.$k;
			$params[':'.$k] = $value;
		}
		
		$sql = "UPDATE ".$this->db->quoteTable($table).' SET '.implode(', ', $updates);
		$conditions = $this->buildConditions($conditions);
		if('' !== $conditions){
			$sql .= ' WHERE '.$conditions;
		}
		
		return $this->setSql($sql)->execute($params);
	}
	
	/**
	 * Delete
	 * 
	 * @param string $table
	 * @param array $values
	 * @param string|array $conditions
	 * @param array $params
	 * @return int Rows affected, false on error
	 */
	public function delete($table, $conditions = '', $params = array()){
		$sql = 'DELETE FROM '.$this->db->quoteTable($table);
		$conditions = $this->buildConditions($conditions);
		if('' !== $conditions){
			$sql .= ' WHERE '.$conditions;
		}
		
		return $this->setSql($sql)->execute($params);
	}
}
