# Privacy Configs and Automatic Management

This page explains OpenViking privacy configs and how they work with skill and session write-time extraction and read-time restore.

## Goal

Privacy configs separate sensitive values (such as `api_key` and `token`) from content (such as skills and sessions). Automatically detected values are saved in the OpenViking user's privacy configs with version history. Content stores placeholders that can be restored from the active config at read time.

Core goals:

- Extract sensitive values at write time and replace them with placeholders
- Restore values at read time according to the content type and policy
- Provide version query, switching, and auditability

Skills and sessions share the same privacy storage, but use different extraction and restore behavior:

| Content | Sensitive Value Extraction | Sensitive Value Restore |
|---|---|---|
| Skill | **Model-based** extraction from the skill body | Replace placeholders with sensitive values from the active config when reading SKILL.md |
| Session | **Rule-based** credential detection in messages; enabled by default | Replace placeholders with sensitive values from the active config when reading message files, archived messages, and externalized tool outputs |

Both methods replace only recognized, matched values; neither guarantees discovery of every sensitive value. Unmatched content remains unchanged.

---

## Core Objects and Storage Layout

Privacy configs are keyed by `category + target_key` within the current user's space.

| Content | `category` | `target_key` |
|---|---|---|
| Skill | `skill` | Skill name |
| Session | `session` | Session ID |

Storage layout in user space:

```
viking://user/{user_id}/privacy/{category}/{target_key}/
├── .meta.json                 # metadata (active_version/latest_version/labels, etc.)
├── current.json               # active version snapshot
└── history/
    ├── version_001.json
    ├── version_002.json
    └── ...
```

`current.json` and `history/version_x.json` contain full privacy value maps (`values`). Session and skill bodies store references to these values (placeholders).

---

## Version Semantics

### upsert

- Each upsert treats incoming `values` as the candidate snapshot
- If `values` is identical to current `values`, no new version is created
- Otherwise a new version is created and activated
- New keys are allowed (no unknown-key rejection)
- `values` is a full snapshot, not a patch; omitted old keys are absent from the new version

### activate

- Sets a historical version as active (writes back to `current.json`)
- Updates `active_version` in `.meta.json`
- Does not rotate or revoke credentials at an external service; it only selects the values OpenViking uses

---

## Skill Privacy Extraction and Restore

### Automatic Extraction (Write Path)

When adding a skill via `add_skill`, OpenViking runs extraction + placeholderization.

```
add_skill
  -> SkillProcessor._sanitize_skill_privacy
    -> extract_skill_privacy_values (LLM extracts values)
    -> placeholderize_skill_content_with_blocks (replace plaintext with placeholders)
    -> privacy.upsert(category="skill", target_key=skill_name, values=...)
  -> write placeholderized SKILL.md
```

#### Key Points

1. **Extraction source**: model returns JSON; `values` is used as privacy key-value pairs.  
2. **Content replacement**: matched plaintext is replaced by placeholders:
   - <code>&#123;&#123;ov_privacy:skill:&#123;skill_name&#125;:&#123;field_name&#125;&#125;&#125;</code>
3. **Block mapping captured**:
   - `original_content_blocks`
   - `replacement_content_blocks`
4. **Persisted content**: matched spans become placeholders; unmatched content remains unchanged. This processes the skill body, not every attached file.

---

### Restore on Read (Load Path)

When reading `SKILL.md`, `FSService.read` attempts placeholder restoration automatically.

```
fs.read(uri)
  -> get_skill_name_from_uri(uri)
  -> privacy.get_current(category="skill", target_key=skill_name)
  -> restore_skill_content(content, skill_name, current.values)
```

#### URI Matching

Current matching is suffix-based: `/skills/{name}/SKILL.md`, so it supports user-scoped skill paths such as:

- `viking://~/skills/{name}/SKILL.md`
- `viking://user/{user_id}/skills/{name}/SKILL.md`

#### restore Rules

`restore_skill_content` behavior:

1. **Placeholder exists in content and value exists and is non-empty**  
   -> replace directly.

2. **Placeholder exists in content but value is missing/empty**  
   -> keep placeholder; add item to `unresolved_entries`.

3. **Config key is non-empty but not referenced by any placeholder in content**  
   -> add to extra-config notice (`Configured but not referenced in content`).

4. If `unresolved_entries` or extra-config entries exist, append notice block:
   - `[Privacy Config Notice]`
   - `Missing config: ...` (if any)
   - `Configured but not referenced in content: ...` (if any)

> Current implementation only runs restore when a `current` privacy config exists for that skill. If no current config exists, no notice is appended.

---

`read` returns restored values, and extra-config notices also include values. Privacy APIs and historical snapshots can return full values as well. This feature does not redact responses or encrypt storage; see [Storage Encryption](./10-encryption.md) for encryption.

## Session Privacy Extraction and Restore

### Automatic Extraction (Write Path)

When writing messages to a session, OpenViking uses `privacy_policy` to control sensitive value extraction and replacement with placeholders. Extraction is enabled by default and runs before tool outputs are externalized and messages are persisted.

```
Session.add_messages_async
  -> resolve privacy_policy and check extract.enabled
  -> extract_session_privacy_values (use rules to detect values and create message copies with placeholders)
  -> privacy.merge_values(category="session", target_key=session_id, values=...)
  -> externalize tool outputs
  -> append messages with placeholders
```

#### Key Points

1. **Extraction source**: rules recognize credentials without calling a model. Supported patterns include `password=...`, `api_key=...`, `Authorization: Bearer ...`, CLI credential arguments, and token formats such as `sk-...`.
2. **Coverage**: extraction processes TextPart text, ContextPart abstracts, and ToolPart inputs, outputs, and externalization error text. Images are not scanned.
3. **Content replacement**: matched sensitive values are replaced by placeholders:
   - <code>&#123;&#123;ov_privacy:session:&#123;session_id&#125;:value_&lt;random-id&gt;&#125;&#125;</code>
   - Keys are random IDs rather than sensitive field names. Existing privacy placeholders and environment variable references are not treated as new credentials.
4. **Persisted content**: new values are merged into the active privacy snapshot before messages are saved, preserving values referenced by earlier messages. If no values are extracted, the privacy config is not written. If extraction or saving the config fails, the batch is rejected without falling back to plaintext storage.

---

### Restore on Read (Load Path)

When reading session message files or externalized tool outputs, `FSService.read` checks `privacy_policy` to determine whether to restore placeholders. Restoration is disabled by default; when enabled, it uses the active privacy config.

```
fs.read(uri)
  -> restore_session_read
    -> get_session_id_from_uri (match a session file)
    -> check for this session's placeholders and read restore.enabled
    -> privacy.get_current(category="session", target_key=session_id)
    -> restore_session_content(content, session_id, current.values)
```

#### URI Matching

Only the following files under the current user's session root are eligible for restore:

- `sessions/{session_id}/messages.jsonl`
- `sessions/{session_id}/history/archive_*/messages.jsonl`
- `sessions/{session_id}/tool-results/{id}/output.txt`

Policy and privacy snapshot lookups are skipped if the path is outside this scope or the content contains no placeholder for that session.

#### Restoration Rules

`restore_session_content` replaces session placeholders with the corresponding values from the active privacy config. Placeholders without a matching value remain unchanged.

> Content restoration runs only when `restore.enabled=true` and a `current` config exists for that session; otherwise, the original content is returned. Restoration affects only the read result and does not write back to the file. Session context, compression summaries, memory extraction, and search results retain placeholders.

### Session Privacy Policy

Set extraction and restore switches through `privacy_policy` when creating a session. The policy cannot be changed afterwards; see [Sessions API](../api/05-sessions.md) for creation parameters and SDK examples.

Deleting a session does not delete its privacy config. This retains values that may still be referenced by extracted memories; it does not enable automatic restore for memory reads.

---

## Relationship with CLI/API

- Management plane: Privacy API/CLI for versions (query, write, rollback)
- Content plane: `read` restores placeholders automatically according to the content's restore policy
- Separation of concerns: content files hold placeholders; the privacy service holds sensitive values and versions

Common commands:

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

## Benefits

- Reduces plaintext sensitive data exposure in skill content
- Version history supports configuration changes after external credential rotation
- Transparent to callers: `read` returns restored executable skill text
- Notice blocks improve troubleshooting when config is incomplete or over-provisioned

---

## Related Documentation

- [Skills](../api/04-skills.md) - Skill write/read API
- [Sessions](../api/05-sessions.md) - Session creation and message writes
- [Privacy API](../api/10-privacy.md) - Privacy config endpoints
- [Context Extraction](./06-extraction.md) - Main extraction pipeline
- [Architecture Overview](./01-architecture.md) - Layering and module relationships
