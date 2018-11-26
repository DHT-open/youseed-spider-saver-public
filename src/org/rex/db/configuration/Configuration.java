/**
 * Copyright 2016 the Rex-Soft Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rex.db.configuration;

import java.io.InputStream;
import java.util.Properties;

import javax.sql.DataSource;

import org.rex.db.datasource.DataSourceManager;
import org.rex.db.dialect.Dialect;
import org.rex.db.dialect.DialectManager;
import org.rex.db.dynamic.javassist.BeanConvertorManager;
import org.rex.db.exception.DBException;
import org.rex.db.exception.DBRuntimeException;
import org.rex.db.exception.ExceptionResourceFactory;
import org.rex.db.listener.DBListener;
import org.rex.db.listener.ListenerManager;
import org.rex.db.logger.Logger;
import org.rex.db.logger.LoggerFactory;
import org.rex.db.transaction.Definition;
import org.rex.db.util.ReflectUtil;
import org.rex.db.util.ResourceUtil;

/**
 * Framework Configuration.
 *
 * @version 1.0, 2016-03-28
 * @since Rexdb-1.0
 */
public class Configuration {
	
	//-----------------------------Singleton instance
	private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);
	
	private static final String DEFAULT_CONFIG_PATH = "rexdb.xml";
	
	private static volatile Configuration instance;
	
	//-----------------------------configuration
	/**
	 * properties
	 */
	private volatile Properties variables;
	
	//--------settings
	/**
	 * language: zh-cn, en
	 */
	private volatile String lang;
	
	/**
	 * Disables all logs?
	 */
	private volatile boolean nolog = false;
	
	/**
	 * Validates SQLs before executing.
	 */
	private volatile boolean validateSql = true;
	
	/**
	 * Retrieves warnings reported by calls on the Connection, Statement and ResultSet object.
	 */
	private volatile boolean checkWarnings = false;
	
	/**
	 * Timeout for each query.
	 */
	private volatile int queryTimeout = -1;
	
	/**
	 * Timeout for each transaction.
	 */
	private volatile int transactionTimeout = -1;
	
	/**
	 * Rolls back the transaction on committing failure.
	 */
	private volatile boolean autoRollback = false;
	
	/**
	 * Transaction isolation.
	 */
	private volatile String transactionIsolation;
	
	/**
	 * Caches beanInfo, parameter types etc in reflections.
	 */
	private volatile boolean reflectCache = true;
	
	/**
	 * Uses dynamic classes instead of reflections.
	 */
	private volatile boolean dynamicClass = true;
	
	/**
	 * Automatically converts Date and Time to java.sql.Timestamp.
	 */
	private volatile boolean dateAdjust = true;
	
	/**
	 * Automatically begins a transaction before batch updating.
	 */
	private volatile boolean batchTransaction = true;
	
	//--------managers
	/**
	 * DataSource Manager.
	 */
	private final DataSourceManager dataSourceManager;
	
	/**
	 * Listener Manager.
	 */
	private final ListenerManager listenerManager;
	
	/**
	 * Dialect Manager.
	 */
	private final DialectManager dialectManager;
	
	static{
		try {
			LOGGER.info("loading default configuration {0}.", DEFAULT_CONFIG_PATH);
			loadDefaultConfiguration();
			LOGGER.info("default configuration {0} loaded.", DEFAULT_CONFIG_PATH);
		} catch (DBException e) {
			LOGGER.warn("could not load default configuration {0} from classpath, rexdb is not initialized, cause {1}", DEFAULT_CONFIG_PATH, e.getMessage());
		}
	}
	
	public Configuration(){
		dataSourceManager = new DataSourceManager();
		listenerManager = new ListenerManager();
		dialectManager = new DialectManager();
	}
	
	public static Configuration getInstance() {
		return instance;
	}
	
	public static Configuration setInstance(Configuration conf) {
		return instance = conf;
	}
	
	/**
	 * Loads the default XML configuration.
	 * 
	 * @throws DBException if the default XML file does not exist, could not parse the file, etc.
	 */
	public synchronized static void loadDefaultConfiguration() throws DBException{
		if(instance != null)
			throw new DBException("DB-F0007", DEFAULT_CONFIG_PATH);
		
		InputStream inputStream = ResourceUtil.getResourceAsStream(DEFAULT_CONFIG_PATH);
		if(inputStream == null){
			LOGGER.warn("could not find configuration {0} in classpath.", DEFAULT_CONFIG_PATH);
		}else
			instance = new XMLConfigurationLoader().load(inputStream);
	}
	
	/**
	 * Loads the default XML configuration from ClassPath.
	 * @param path the path of the XML file.
	 * @throws DBException if the default XML file does not exist, could not parse the file, etc.
	 */
	public synchronized static void loadConfigurationFromClasspath(String path) throws DBException{
		if(instance != null)
			throw new DBException("DB-F0007", path);
		
		LOGGER.info("loading configuration {0} from classpath.", path);
		instance = new XMLConfigurationLoader().loadFromClasspath(path);
		LOGGER.info("configuration {0} loaded.", path);
	}
	
	/**
	 * Loads the default XML configuration from ClassPath.
	 * @param path the absolute path of the XML file .
	 * @throws DBException if the default XML file does not exist, could not parse the file, etc.
	 */
	public synchronized static void loadConfigurationFromFileSystem(String path) throws DBException{
		if(instance != null)
			throw new DBException("DB-F0007", path);
		
		LOGGER.info("loading configuration {0} from file system.", path);
		instance = new XMLConfigurationLoader().loadFromFileSystem(path);
		LOGGER.info("configuration {0} loaded.", path);
	}

	/**
	 * Returns the current configuration.
	 */
	public static Configuration getCurrentConfiguration() throws DBRuntimeException{
		if(instance == null){
			try {
				loadDefaultConfiguration();
			} catch (DBException e) {
				throw new DBRuntimeException(e);
			}
		}
		
		if(instance == null)
			throw new DBRuntimeException("DB-F0008", DEFAULT_CONFIG_PATH);
			
		return instance;
	}
	
	
	//---------------------------
	public void applySettings(){
		//lang
		if(lang != null){
			ExceptionResourceFactory.getInstance().setLang(lang);
		}
		
		//reflectCache
		if(!reflectCache){
			ReflectUtil.setCacheEnabled(false);
		}
		
		//nolog
		if(nolog){
			LoggerFactory.setNolog(true);
		}

	}
	
	//---------------------------
	public void addVariables(Properties variables) {
		if(this.variables == null) 
			this.variables = variables;
		else
			this.variables.putAll(variables);
	}

	public Properties getVariables() {
		return variables;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}
	
	public boolean isNolog() {
		return nolog;
	}

	public void setNolog(boolean nolog) {
		this.nolog = nolog;
	}
	
	public boolean isValidateSql() {
		return validateSql;
	}

	public void setValidateSql(boolean validateSql) {
		LOGGER.info("sql validate has switched to {0}.", validateSql);
		this.validateSql = validateSql;
	}

	public boolean isCheckWarnings() {
		return checkWarnings;
	}

	public void setCheckWarnings(boolean checkWarnings) {
		LOGGER.info("check warnings has switched to {0}.", validateSql);
		this.checkWarnings = checkWarnings;
	}
	
	public int getQueryTimeout() {
		return queryTimeout;
	}

	public void setQueryTimeout(int queryTimeout) {
		this.queryTimeout = queryTimeout;
	}

	public int getTransactionTimeout() {
		return transactionTimeout;
	}

	public void setTransactionTimeout(int transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
	}

	public boolean isAutoRollback() {
		return autoRollback;
	}

	public void setAutoRollback(boolean autoRollback) {
		this.autoRollback = autoRollback;
	}

	public String getTransactionIsolation() {
		return transactionIsolation;
	}

	public void setTransactionIsolation(String transactionIsolation) {
		this.transactionIsolation = Definition.ISOLATION_CONSTANT_PREFIX + '_' +transactionIsolation;
	}
	
	public boolean isReflectCache() {
		return reflectCache;
	}

	public void setReflectCache(boolean reflectCache) {
		this.reflectCache = reflectCache;
	}
	
	public boolean isDynamicClass() {
		return dynamicClass;
	}

	public void setDynamicClass(boolean dynamicClass) {
		if(dynamicClass){//test dynamic
			try{
				BeanConvertorManager m = new BeanConvertorManager();
				BeanConvertorManager.getConvertor(Configuration.class);
				this.dynamicClass = true;
			}catch(Throwable e){
				this.dynamicClass = false;
				LOGGER.warn("dynamic class setting is true, but could not pass the validation test, now automatically switch to false, {0}", e, e.getMessage());
			}
		}else
			this.dynamicClass = false;
	}

	public boolean isDateAdjust() {
		return dateAdjust;
	}

	public void setDateAdjust(boolean dateAdjust) {
		this.dateAdjust = dateAdjust;
	}

	public boolean isBatchTransaction() {
		return batchTransaction;
	}

	public void setBatchTransaction(boolean batchTransaction) {
		this.batchTransaction = batchTransaction;
	}

	//-----------
	public void setDefaultDataSource(DataSource dataSource){
		dataSourceManager.setDefault(dataSource);
	}
	
	public void setDataSource(String id, DataSource dataSource){
		dataSourceManager.add(id, dataSource);
	}

	public DataSourceManager getDataSourceManager() {
		return dataSourceManager;
	}
	
	public void addListener(DBListener listener){
		listenerManager.registe(listener);
	}

	public ListenerManager getListenerManager() {
		return listenerManager;
	}
	
	public void addDialect(DataSource dataSource, Dialect dialect){
		dialectManager.setDialect(dataSource, dialect);;
	}

	public DialectManager getDialectManager() {
		return dialectManager;
	}

	public String toString() {
		return "Configuration [variables=" + variables + ", lang=" + lang + ", dataSourceManager=" + dataSourceManager + ", listenerManager="
				+ listenerManager + ", dialectManager=" + dialectManager + "]";
	}
	
}
