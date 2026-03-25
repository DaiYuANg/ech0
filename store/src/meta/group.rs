use redb::{ReadableDatabase, ReadableTable};

use super::*;

impl<TopicCodec, BrokerCodec, PartitionCodec> ConsumerGroupStore
  for RedbMetadataStore<TopicCodec, BrokerCodec, PartitionCodec>
where
  TopicCodec: Codec<TopicConfig>,
  BrokerCodec: Codec<BrokerState>,
  PartitionCodec: Codec<LocalPartitionState>,
{
  fn save_group_member(&self, member: &crate::ConsumerGroupMember) -> Result<()> {
    let payload = serde_json::to_vec(member).map_err(|err| {
      crate::StoreError::Codec(format!("failed to encode consumer group member: {err}"))
    })?;
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
      let key = Self::consumer_group_member_key(&member.group, &member.member_id);
      table.insert(key.as_str(), payload.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_group_member(
    &self,
    group: &str,
    member_id: &str,
  ) -> Result<Option<crate::ConsumerGroupMember>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
    let key = Self::consumer_group_member_key(group, member_id);
    let Some(value) = table.get(key.as_str())? else {
      return Ok(None);
    };
    serde_json::from_slice(value.value())
      .map(Some)
      .map_err(|err| {
        crate::StoreError::Corruption(format!(
          "invalid consumer group member payload for {group}/{member_id}: {err}"
        ))
      })
  }

  fn list_group_members(&self, group: &str) -> Result<Vec<crate::ConsumerGroupMember>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
    let mut members: Vec<crate::ConsumerGroupMember> = Vec::new();
    for entry in table.iter()? {
      let (key, value) = entry?;
      let Some((entry_group, _entry_member_id)) =
        Self::parse_consumer_group_member_key(key.value())
      else {
        continue;
      };
      if entry_group != group {
        continue;
      }
      members.push(serde_json::from_slice(value.value()).map_err(|err| {
        crate::StoreError::Corruption(format!(
          "invalid consumer group member payload for group {group}: {err}"
        ))
      })?);
    }
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    Ok(members)
  }

  fn delete_group_member(&self, group: &str, member_id: &str) -> Result<()> {
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
      let key = Self::consumer_group_member_key(group, member_id);
      table.remove(key.as_str())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn delete_expired_group_members(&self, now_ms: u64) -> Result<usize> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
    let mut expired_keys = Vec::new();
    for entry in table.iter()? {
      let (key, value) = entry?;
      let member: crate::ConsumerGroupMember =
        serde_json::from_slice(value.value()).map_err(|err| {
          crate::StoreError::Corruption(format!(
            "invalid consumer group member payload while sweeping expirations: {err}"
          ))
        })?;
      if member.is_expired_at_ms(now_ms) {
        expired_keys.push(key.value().to_owned());
      }
    }
    drop(table);
    drop(read_txn);

    if expired_keys.is_empty() {
      return Ok(0);
    }

    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSUMER_GROUP_MEMBERS)?;
      for key in &expired_keys {
        table.remove(key.as_str())?;
      }
    }
    write_txn.commit()?;
    Ok(expired_keys.len())
  }

  fn save_group_assignment(&self, assignment: &crate::ConsumerGroupAssignment) -> Result<()> {
    let payload = serde_json::to_vec(assignment).map_err(|err| {
      crate::StoreError::Codec(format!("failed to encode consumer group assignment: {err}"))
    })?;
    let write_txn = self.db.begin_write()?;
    {
      let mut table = write_txn.open_table(CONSUMER_GROUP_ASSIGNMENTS)?;
      let key = Self::consumer_group_assignment_key(&assignment.group);
      table.insert(key.as_str(), payload.as_slice())?;
    }
    write_txn.commit()?;
    Ok(())
  }

  fn load_group_assignment(&self, group: &str) -> Result<Option<crate::ConsumerGroupAssignment>> {
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(CONSUMER_GROUP_ASSIGNMENTS)?;
    let key = Self::consumer_group_assignment_key(group);
    let Some(value) = table.get(key.as_str())? else {
      return Ok(None);
    };
    serde_json::from_slice(value.value())
      .map(Some)
      .map_err(|err| {
        crate::StoreError::Corruption(format!(
          "invalid consumer group assignment payload for {group}: {err}"
        ))
      })
  }
}
