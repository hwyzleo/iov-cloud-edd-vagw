# Graph Report - iov-cloud-edd-vagw  (2026-06-27)

## Corpus Check
- 32 files · ~12,638 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 143 nodes · 175 edges · 21 communities detected
- Extraction: 71% EXTRACTED · 29% INFERRED · 0% AMBIGUOUS · INFERRED: 51 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]

## God Nodes (most connected - your core abstractions)
1. `SessionServiceTest` - 14 edges
2. `BindingServiceTest` - 8 edges
3. `SessionService` - 8 edges
4. `AuthAclServiceTest` - 7 edges
5. `UplinkServiceTest` - 6 edges
6. `MqttClientManager` - 6 edges
7. `BindingServiceImpl` - 6 edges
8. `DownlinkServiceTest` - 5 edges
9. `MqttEventControllerTest` - 4 edges
10. `RouteService` - 4 edges

## Surprising Connections (you probably didn't know these)
- None detected - all connections are within the same source files.

## Communities

### Community 0 - "Community 0"
Cohesion: 0.16
Nodes (2): SessionService, SessionServiceTest

### Community 1 - "Community 1"
Cohesion: 0.17
Nodes (6): UplinkKafkaProducer, RouteService, fail(), success(), UplinkService, UplinkServiceTest

### Community 2 - "Community 2"
Cohesion: 0.18
Nodes (3): VehicleStatusController, DownlinkService, DownlinkServiceTest

### Community 3 - "Community 3"
Cohesion: 0.13
Nodes (3): BindingService, BindingServiceImpl, BindingServiceTest

### Community 4 - "Community 4"
Cohesion: 0.29
Nodes (4): MqttAuthControllerTest, allow(), AuthAclService, deny()

### Community 5 - "Community 5"
Cohesion: 0.31
Nodes (2): MqttEventController, MqttEventControllerTest

### Community 6 - "Community 6"
Cohesion: 0.25
Nodes (1): AuthAclServiceTest

### Community 7 - "Community 7"
Cohesion: 0.4
Nodes (1): MqttClientManager

### Community 8 - "Community 8"
Cohesion: 0.4
Nodes (1): BindingService

### Community 9 - "Community 9"
Cohesion: 0.67
Nodes (1): Application

### Community 10 - "Community 10"
Cohesion: 0.67
Nodes (1): AppConfig

### Community 11 - "Community 11"
Cohesion: 0.67
Nodes (1): MqttAuthController

### Community 12 - "Community 12"
Cohesion: 0.67
Nodes (1): VehicleCommandController

### Community 13 - "Community 13"
Cohesion: 0.67
Nodes (1): DownlinkKafkaConsumer

### Community 14 - "Community 14"
Cohesion: 0.67
Nodes (2): AclRule, MqttAuthResponse

### Community 15 - "Community 15"
Cohesion: 1.0
Nodes (2): SessionInfo, Serializable

### Community 16 - "Community 16"
Cohesion: 1.0
Nodes (1): DownlinkCommandRequest

### Community 17 - "Community 17"
Cohesion: 1.0
Nodes (1): MqttEventRequest

### Community 18 - "Community 18"
Cohesion: 1.0
Nodes (1): DownlinkCommandResponse

### Community 19 - "Community 19"
Cohesion: 1.0
Nodes (1): VehicleStatusResponse

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (1): MqttAuthRequest

## Knowledge Gaps
- **7 isolated node(s):** `DownlinkCommandRequest`, `MqttEventRequest`, `MqttAuthResponse`, `AclRule`, `DownlinkCommandResponse` (+2 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 0`** (20 nodes): `SessionService.java`, `SessionService`, `.getSessionByDeviceSn()`, `.getSessionByVin()`, `.isOnlineByDeviceSn()`, `.isOnlineByVin()`, `.onConnected()`, `SessionServiceTest`, `.getSessionByDeviceSn_shouldReturnEmptyWhenNotExists()`, `.getSessionByDeviceSn_shouldReturnSessionWhenExists()`, `.getSessionByVin_shouldReturnEmptyWhenNotBound()`, `.getSessionByVin_shouldReturnSessionWhenBound()`, `.isOnlineByDeviceSn_shouldReturnFalseWhenNoSession()`, `.isOnlineByDeviceSn_shouldReturnTrueWhenOnline()`, `.isOnlineByVin_shouldReturnFalseWhenNotBound()`, `.isOnlineByVin_shouldReturnTrueWhenOnline()`, `.onConnected_shouldSaveSessionWhenVinNotBound()`, `.onConnected_shouldSaveSessionWithDeviceSn()`, `.setUp()`, `SessionServiceTest.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 5`** (9 nodes): `MqttEventController`, `.handleEvent()`, `MqttEventControllerTest`, `.handleEvent_blankDeviceSn_shouldIgnore()`, `.handleEvent_connected_shouldCallSessionService()`, `.handleEvent_disconnected_shouldCallSessionService()`, `MqttEventController.java`, `.onDisconnected()`, `MqttEventControllerTest.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 6`** (8 nodes): `AuthAclServiceTest`, `.authenticate_emptyDeviceSn_shouldDeny()`, `.authenticate_invalidFormat_shouldDeny()`, `.authenticate_lowercaseDeviceSn_shouldNormalize()`, `.authenticate_nullDeviceSn_shouldDeny()`, `.authenticate_validDeviceSnWithBinding_shouldAllow()`, `.authenticate_validDeviceSnWithoutBinding_shouldDeny()`, `AuthAclServiceTest.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 7`** (6 nodes): `MqttClientManager.java`, `MqttClientManager`, `.connect()`, `.handleMessage()`, `.init()`, `.shutdown()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 8`** (5 nodes): `BindingService.java`, `BindingService`, `.isValidAndBound()`, `.resolveDeviceSn()`, `.resolveVin()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 9`** (3 nodes): `Application.java`, `Application`, `.main()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 10`** (3 nodes): `AppConfig`, `.objectMapper()`, `AppConfig.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 11`** (3 nodes): `MqttAuthController`, `.authenticate()`, `MqttAuthController.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 12`** (3 nodes): `VehicleCommandController`, `.sendCommand()`, `VehicleCommandController.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 13`** (3 nodes): `DownlinkKafkaConsumer`, `.consume()`, `DownlinkKafkaConsumer.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 14`** (3 nodes): `AclRule`, `MqttAuthResponse`, `MqttAuthResponse.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 15`** (3 nodes): `SessionInfo`, `SessionInfo.java`, `Serializable`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 16`** (2 nodes): `DownlinkCommandRequest`, `DownlinkCommandRequest.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 17`** (2 nodes): `MqttEventRequest`, `MqttEventRequest.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 18`** (2 nodes): `DownlinkCommandResponse`, `DownlinkCommandResponse.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 19`** (2 nodes): `VehicleStatusResponse`, `VehicleStatusResponse.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 20`** (2 nodes): `MqttAuthRequest`, `MqttAuthRequest.java`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `RouteService` connect `Community 1` to `Community 2`?**
  _High betweenness centrality (0.108) - this node is a cross-community bridge._
- **Why does `SessionService` connect `Community 0` to `Community 2`, `Community 5`?**
  _High betweenness centrality (0.076) - this node is a cross-community bridge._
- **Why does `MqttClientManager` connect `Community 7` to `Community 2`?**
  _High betweenness centrality (0.052) - this node is a cross-community bridge._
- **What connects `DownlinkCommandRequest`, `MqttEventRequest`, `MqttAuthResponse` to the rest of the system?**
  _7 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 3` be split into smaller, more focused modules?**
  _Cohesion score 0.13 - nodes in this community are weakly interconnected._