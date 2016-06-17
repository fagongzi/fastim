链接管理
--------
## 链接创建
* client和router建立链接后，router针对一个client生成一个全局唯一的session_id。
* router把session_id和router自己的internalAddr绑定，并且存储到redis中，默认的超时时间是1小时。到达1小时后，检查链接是否存在，如果存在，重新绑定。如果不存在，则绑定关系在redis中失效。

## 链接断开
* router感知到链接断开或者客户端心跳超时，router删除redis中的绑定关系。
* router发送会话关闭消息给后端的相关service

## router异常宕机
* client感知到链接断开
* 在剩余可用的router中选择一个router进行重连
* client的绑定关系在1hour后自动失效
* 消息可靠性依靠报文协议保证

## router优雅停机
* 停止accept新tcp链接，并且回复change_router报文给client
* 到老链接的报文，不处理，直接回复change_router
* client收到change_router必须停止向当前链接继续发送报文，并且更换链接router，session保持功能有具体业务实现
* router收到后端的消息，不处理，直接丢弃。消息可靠性依靠报文协议保证
* router广播给所有client发送change_router后，等待一小段时间退出