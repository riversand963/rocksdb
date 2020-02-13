//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit_handler.h"

#include "monitoring/persistent_stats_history.h"

namespace rocksdb {

VersionEditHandler::VersionEditHandler(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    VersionSet* version_set)
    : column_families_(column_families), version_set_(version_set) {}

Status VersionEditHandler::Initialize() {
  for (const auto& cf_desc : column_families_) {
    name_to_options_.emplace(cf_desc.name, cf_desc.options);
  }
  auto default_cf_iter = name_to_options_.find(kDefaultColumnFamilyName);
  Status s;
  if (default_cf_iter == name_to_options_.end()) {
    s = Status::InvalidArgument("Default column family not specified");
  }
  if (s.ok()) {
    VersionEdit default_cf_edit;
    default_cf_edit.AddColumnFamily(kDefaultColumnFamilyName);
    default_cf_edit.SetColumnFamily(0);
    ColumnFamilyData* cfd __attribute__((__unused__)) =
        CreateCfAndInit(default_cf_iter->second, &default_cf_edit);
    assert(cfd != nullptr);
  }
  return s;
}

Status VersionEditHandler::Iterate(log::Reader& reader, std::string* db_id) {
  Slice record;
  std::string scratch;
  Status s;
  size_t recovered_edits = 0;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    if (edit.has_db_id_) {
      db_id_ = edit.GetDbId();
      if (db_id != nullptr) {
        *db_id = db_id_;
      }
    }
    s = read_buffer_.AddEdit(&edit);
    if (!s.ok()) {
      break;
    }
    ColumnFamilyData* cfd = nullptr;
    if (edit.is_in_atomic_group_) {
      if (read_buffer_.IsFull()) {
        for (auto& e : read_buffer_.replay_buffer()) {
          s = ApplyOneVersionEditToBuilder(e, &cfd);
          if (!s.ok()) {
            break;
          }
          ++recovered_edits;
        }
        if (!s.ok()) {
          break;
        }
        read_buffer_.Clear();
      }
    } else {
      s = ApplyOneVersionEditToBuilder(edit, &cfd);
      if (s.ok()) {
        ++recovered_edits;
      }
    }
  }
  if (!s.ok()) {
    read_buffer_.Clear();
  }
  return s;
}

Status VersionEditHandler::ApplyOneVersionEditToBuilder(
    VersionEdit& edit, ColumnFamilyData** cfd) {
  Status s;
  if (edit.is_column_family_add_) {
    s = OnColumnFamilyAdd(edit, cfd);
  } else if (edit.is_column_family_drop_) {
    s = OnColumnFamilyDrop(edit, cfd);
  } else {
    s = OnNonCfOperation(edit, cfd);
  }
  if (s.ok()) {
    assert(cfd != nullptr);
    s = ExtractInfoFromVersionEdit(*cfd, edit);
  }
  return s;
}

Status VersionEditHandler::OnColumnFamilyAdd(VersionEdit& edit,
                                             ColumnFamilyData** cfd) {
  bool cf_in_not_found = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &cf_in_not_found, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  if (cf_in_builders || cf_in_not_found) {
    return Status::Corruption("MANIFEST adding the same column family twice: " +
                              edit.column_family_name_);
  }
  auto cf_options = name_to_options_.find(edit.column_family_name_);
  // implicitly add persistent_stats column family without requiring user
  // to specify
  ColumnFamilyData* tmp_cfd = nullptr;
  bool is_persistent_stats_column_family =
      edit.column_family_name_.compare(kPersistentStatsColumnFamilyName) == 0;
  if (cf_options == name_to_options_.end() &&
      !is_persistent_stats_column_family) {
    column_families_not_found_.emplace(edit.column_family_,
                                       edit.column_family_name_);
  } else {
    if (is_persistent_stats_column_family) {
      ColumnFamilyOptions cfo;
      OptimizeForPersistentStats(&cfo);
      tmp_cfd = CreateCfAndInit(cfo, &edit);
    } else {
      tmp_cfd = CreateCfAndInit(cf_options->second, &edit);
    }
    *cfd = tmp_cfd;
  }
  return Status::OK();
}

Status VersionEditHandler::OnColumnFamilyDrop(VersionEdit& edit,
                                              ColumnFamilyData** cfd) {
  bool cf_in_not_found = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &cf_in_not_found, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  ColumnFamilyData* tmp_cfd = nullptr;
  Status s;
  if (cf_in_builders) {
    auto builder_iter = builders_.find(edit.column_family_);
    assert(builder_iter != builders_.end());
    builders_.erase(builder_iter);
    tmp_cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
        edit.column_family_);
    assert(tmp_cfd);
    if (tmp_cfd->UnrefAndTryDelete()) {
      tmp_cfd = nullptr;
    }
  } else if (cf_in_not_found) {
    column_families_not_found_.erase(edit.column_family_);
  } else {
    s = Status::Corruption("MANIFEST - dropping non-existing column family");
  }
  *cfd = tmp_cfd;
  return s;
}

Status VersionEditHandler::OnNonCfOperation(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  bool cf_in_not_found = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &cf_in_not_found, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  Status s;
  if (!cf_in_not_found) {
    if (!cf_in_builders) {
      s = Status::Corruption(
          "MANIFEST record referencing unknown column family");
    }
    ColumnFamilyData* tmp_cfd = nullptr;
    if (s.ok()) {
      tmp_cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
          edit.column_family_);
      assert(tmp_cfd != nullptr);
      auto builder_iter = builders_.find(edit.column_family_);
      assert(builder_iter != builders_.end());
      s = builder_iter->second->version_builder()->Apply(&edit);
    }
    *cfd = tmp_cfd;
  }
  return s;
}

void VersionEditHandler::CheckColumnFamilyId(const VersionEdit& edit,
                                             bool* cf_in_not_found,
                                             bool* cf_in_builders) const {
  assert(cf_in_not_found != nullptr);
  assert(cf_in_builders != nullptr);
  // Not found means that user didn't supply that column
  // family option AND we encountered column family add
  // record. Once we encounter column family drop record,
  // we will delete the column family from
  // column_families_not_found.
  bool in_not_found = column_families_not_found_.find(edit.column_family_) !=
                      column_families_not_found_.end();
  // in builders means that user supplied that column family
  // option AND that we encountered column family add record
  bool in_builders = builders_.find(edit.column_family_) != builders_.end();
  // They cannot both be true
  assert(!(in_not_found && in_builders));
  *cf_in_not_found = in_not_found;
  *cf_in_builders = in_builders;
}

ColumnFamilyData* VersionEditHandler::CreateCfAndInit(
    const ColumnFamilyOptions& cf_options, VersionEdit* edit) {
  ColumnFamilyData* cfd = version_set_->CreateColumnFamily(cf_options, edit);
  assert(cfd != nullptr);
  cfd->set_initialized();
  return cfd;
}

Status VersionEditHandler::ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                                      const VersionEdit& edit) {
  Status s;
  if (cfd != nullptr) {
    if (edit.has_db_id_) {
      version_edit_params_.SetDBId(edit.db_id_);
    }
    if (edit.has_log_number_) {
      if (cfd->GetLogNumber() > edit.log_number_) {
        ROCKS_LOG_WARN(
            version_set_->db_options()->info_log,
            "MANIFEST corruption detected, but ignored - Log numbers in "
            "records NOT monotonically increasing");
      } else {
        cfd->SetLogNumber(edit.log_number_);
        version_edit_params_.SetLogNumber(edit.log_number_);
      }
    }
    if (edit.has_comparator_ &&
        edit.comparator_ != cfd->user_comparator()->Name()) {
      s = Status::InvalidArgument(
          cfd->user_comparator()->Name(),
          "does not match existing comparator " + edit.comparator_);
    }
  }

  if (s.ok()) {
    if (edit.has_prev_log_number_) {
      version_edit_params_.SetPrevLogNumber(edit.prev_log_number_);
    }
    if (edit.has_next_file_number_) {
      version_edit_params_.SetNextFile(edit.next_file_number_);
    }
    if (edit.has_max_column_family_) {
      version_edit_params_.SetMaxColumnFamily(edit.max_column_family_);
    }
    if (edit.has_min_log_number_to_keep_) {
      version_edit_params_.min_log_number_to_keep_ =
          std::max(version_edit_params_.min_log_number_to_keep_,
                   edit.min_log_number_to_keep_);
    }
    if (edit.has_last_sequence_) {
      version_edit_params_.SetLastSequence(edit.last_sequence_);
    }
    if (!version_edit_params_.has_prev_log_number_) {
      version_edit_params_.SetPrevLogNumber(0);
    }
  }
  return s;
}

}  // namespace rocksdb
