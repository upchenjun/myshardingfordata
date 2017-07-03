# myshardingfordata
完全面向对象的数据库自动读写分离、分库分表中间件


启用事务支持
一、配置类上面加上
@EnableAspectJAutoProxy(exposeProxy = true, proxyTargetClass = true)

二、配置类中配置
  @Bean
	public TransManager transManager() {
		return new TransManager(connectionManager());
	}
