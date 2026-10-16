# 隐私配置与自动管理

本文介绍 OpenViking 的隐私配置能力，以及它与技能/会话写入（提取）和读取（加载还原）的协作机制。

## 目标

隐私配置用于把敏感值（如 `api_key`、`token`）从内容（如技能、会话）中分离出来。被自动扫描的敏感信息，保存在 OpenViking 用户空间的隐私配置中并保留版本记录，内容则保存占位符，读取时可根据当前生效配置还原。

核心目标：

- 写入时自动提取敏感值并占位符化
- 读取时按内容类型和策略还原
- 支持版本查询、切换与审计

技能（Skill）和会话（Session）复用同一套隐私存储，隐私敏感值的提取与还原行为有所不同：

| 内容 | 敏感值的提取方式 | 敏感值的读取还原 |
|---|---|---|
| Skill | 通过**模型**从技能正文中提取 | 读取 SKILL.md 时，根据当前生效配置将占位符还原为敏感原值 |
| Session | 通过**规则**识别消息中的凭据，默认开启 | 读取消息文件、历史消息和外置工具输出时，根据当前生效配置，将占位符还原为敏感原值 |

两种方式都只替换识别并匹配成功的值，不能保证发现所有敏感信息；未匹配内容保持原样。

---

## 核心对象与存储结构

隐私配置在当前用户空间内按二级键组织：`category + target_key`。

| 内容 | `category` | `target_key` |
|---|---|---|
| Skill | `skill` | Skill 名称 |
| Session | `session` | Session ID |

用户空间下的存储路径：

```
viking://user/{user_id}/privacy/{category}/{target_key}/
├── .meta.json                 # 元信息（active_version/latest_version/labels 等）
├── current.json               # 当前生效版本快照
└── history/
    ├── version_001.json
    ├── version_002.json
    └── ...
```

`current.json` 与 `history/version_x.json` 都是完整的隐私值字典（`values` 整包）。而 Session 或 Skill 中的正文只会保存这些隐私值的引用（即占位符）。

---

## 版本语义

### upsert（写入）

- 每次 upsert 都以传入 `values` 作为“新快照候选”
- 如果与当前版本 `values` 完全一致，则不新建版本，直接返回当前版本
- 否则创建新版本并自动设为当前生效版本
- 允许新增 key（不会因“未知 key”报错）
- `values` 是完整快照，不是部分更新；省略的旧 key 不会保留在新版本中

### activate（激活）

- 将历史版本设置为当前生效版本（写回 `current.json`）
- 更新 `.meta.json` 中的 `active_version`
- 不会轮换或撤销第三方服务中的密钥；这里只切换 OpenViking 使用的值

---

## 技能的隐私提取与读取还原

### 自动提取（写入链路）

当通过 `add_skill` 写入 Skill 时，会走隐私提取与占位符化流程。

```
add_skill
  -> SkillProcessor._sanitize_skill_privacy
    -> extract_skill_privacy_values (LLM 抽取 values)
    -> placeholderize_skill_content_with_blocks (明文替换为占位符)
    -> privacy.upsert(category="skill", target_key=skill_name, values=...)
  -> 写入占位符化后的 SKILL.md
```

#### 关键点

1. **抽取结果来源**：模型返回 JSON，读取 `values` 字段作为隐私键值。  
2. **内容替换**：原文中命中的敏感片段会替换为占位符：
   - <code>&#123;&#123;ov_privacy:skill:&#123;skill_name&#125;:&#123;field_name&#125;&#125;&#125;</code>
3. **保留块映射**：会同时记录：
   - `original_content_blocks`
   - `replacement_content_blocks`
4. **写盘结果**：匹配成功的片段以占位符保存，未匹配内容保持原样。该流程处理技能正文，不等于扫描并清理所有附件。

---

### 加载还原（读取链路）

读取 `SKILL.md` 时，`FSService.read` 会尝试自动还原占位符。

```
fs.read(uri)
  -> get_skill_name_from_uri(uri)
  -> privacy.get_current(category="skill", target_key=skill_name)
  -> restore_skill_content(content, skill_name, current.values)
```

#### URI 匹配

当前支持通过后缀识别 Skill：`/skills/{name}/SKILL.md`，可兼容 user-scoped skill 路径，例如：

- `viking://~/skills/{name}/SKILL.md`
- `viking://user/{user_id}/skills/{name}/SKILL.md`

#### restore 规则

`restore_skill_content` 的行为如下：

1. **content 中有占位符，且 privacy value 存在且非空**  
   -> 直接替换为对应值。

2. **content 中有占位符，但 privacy value 缺失或为空**  
   -> 保留占位符不替换，并记入 `unresolved_entries`。

3. **privacy 中存在 key 且非空，但 content 中没有对应占位符**  
   -> 记入“额外配置”提示（`Configured but not referenced in content`）。

4. 当存在 `unresolved_entries` 或“额外配置”时，会在内容末尾追加：
   - `[Privacy Config Notice]`
   - `Missing config: ...`（如有）
   - `Configured but not referenced in content: ...`（如有）

> 仅当该 skill 已有 `current` 配置时才会进入 restore。若没有当前配置，不会追加 notice。

---

`read` 返回的是还原后的值；“额外配置”提示也会包含配置值。Privacy API 和历史快照同样可以返回完整值，该功能不等于响应脱敏或存储加密。需要加密存储时，另见[存储加密](./10-encryption.md)。

## 会话的隐私提取与读取还原

### 自动提取（写入链路）

向 Session 写入消息时，会根据 `privacy_policy` 执行隐私提取与占位符化。提取默认开启，发生在工具输出外置和消息落盘之前。

```
Session.add_messages_async
  -> 解析 privacy_policy，检查 extract.enabled
  -> extract_session_privacy_values（规则识别，生成占位符化的消息副本）
  -> privacy.merge_values(category="session", target_key=session_id, values=...)
  -> 工具输出外置
  -> 追加占位符化后的消息
```

#### 关键点

1. **提取结果来源**：通过规则识别凭据，不调用模型。支持 `password=...`、`api_key=...`、`Authorization: Bearer ...`、CLI 凭据参数和 `sk-...` 等 Token 格式。
2. **覆盖范围**：处理 TextPart 文本、ContextPart 摘要，以及 ToolPart 的输入、输出和外置错误文本；不扫描图片。
3. **内容替换**：命中的敏感值替换为占位符：
   - <code>&#123;&#123;ov_privacy:session:&#123;session_id&#125;:value_&lt;随机ID&gt;&#125;&#125;</code>
   - key 使用随机 ID，不使用敏感字段名。已有 Privacy 占位符和环境变量引用不会被当作新的凭据。
4. **写盘结果**：先把新值合并进当前 Privacy 快照，保留旧消息引用的值，再保存消息。没有提取到值时，不写入 Privacy 配置；提取或保存配置失败时，拒绝本批写入，不回退到明文存储。

---

### 加载还原（读取链路）

读取 Session 消息文件或外置工具输出时，`FSService.read` 会根据 `privacy_policy` 尝试还原占位符。还原默认关闭，开启后使用当前生效的 Privacy 配置。

```
fs.read(uri)
  -> restore_session_read
    -> get_session_id_from_uri（匹配 Session 文件）
    -> 检查是否包含该 Session 的占位符，读取 restore.enabled
    -> privacy.get_current(category="session", target_key=session_id)
    -> restore_session_content(content, session_id, current.values)
```

#### URI 匹配

当前只还原当前用户 Session 根目录下的以下文件：

- `sessions/{session_id}/messages.jsonl`
- `sessions/{session_id}/history/archive_*/messages.jsonl`
- `sessions/{session_id}/tool-results/{id}/output.txt`

路径不在支持范围内，或内容没有该 Session 的占位符时，不查询策略和 Privacy 快照。

#### restore 规则

`restore_session_content` 同样按占位符查找当前配置并还原；找不到对应值时保留占位符。

> 仅当 `restore.enabled=true` 且该 Session 已有 `current` 配置时才会进入内容还原；否则返回原内容。只还原读取结果，不写回文件，Session context、压缩摘要、记忆抽取和检索结果仍保留占位符。

### 会话的隐私配置

创建 Session 时通过 `privacy_policy` 设置提取与还原开关，之后不再修改，具体创建参数和 SDK 示例见 [Session API](../api/05-sessions.md)。

删除 Session 不会删除对应的 Privacy 配置，以保留已抽取记忆可能引用的原值；这不代表读取记忆时会自动还原。

---

## 与 CLI/API 的关系

- 管理面：通过 Privacy API/CLI 管理版本（查询、写入、回滚）
- 内容面：通过 `read` 自动恢复占位符
- 两者解耦：内容文件负责占位符，隐私服务负责敏感值与版本

常用命令：

```bash
ov privacy categories
ov privacy list skill
ov privacy get skill search-web
ov privacy upsert skill search-web --values-json '{"api_key":"..."}'
ov privacy activate skill search-web 1
ov read viking://~/skills/search-web/SKILL.md

ov privacy list session
ov privacy get session demo
ov read viking://~/sessions/demo/messages.jsonl
```

---

## 设计收益

- 降低明文敏感信息在技能正文中的暴露风险
- 为外部密钥轮换后的配置切换保留版本记录
- 对上层调用透明：`read` 即可拿到还原后的可执行技能文本
- 对不完整配置提供可观测提示，便于排障

---

## 相关文档

- [技能](../api/04-skills.md) - Skill 写入与读取 API
- [会话](../api/05-sessions.md) - Session 创建、消息写入
- [隐私配置 API](../api/10-privacy.md) - 隐私配置端点说明
- [上下文提取](./06-extraction.md) - 内容提取主流程
- [架构概述](./01-architecture.md) - 系统分层与模块关系
