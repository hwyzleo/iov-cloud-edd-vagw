package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

/**
 * 认证发布上下文来源（EDD-VAGW-DSN-CR-011 §3.2）。
 * <p>
 * 当前 VAGW MQTT 客户端（Paho v3.1.1）共享订阅路径无法携带 EMQX publisher 原始元数据，
 * 因此采用 {@link #ACL_SESSION_CORRELATED}：EMQX ACL 以已验证客户端证书 CN 强制
 * Topic device key == 认证身份，VAGW 再以准入/连接事件建立的会话不可变快照交叉校验。
 * 该模式不信任 MQTT payload 或用户属性自报身份；后续若 EMQX Rule/Hook 可注入
 * publisher metadata，可平滑新增来源而不改变下游契约。
 * </p>
 */
public enum KeyprovContextSource {
    /** Topic 认证身份（EMQX ACL）与会话注册表快照关联得到的已认证上下文。 */
    ACL_SESSION_CORRELATED
}
