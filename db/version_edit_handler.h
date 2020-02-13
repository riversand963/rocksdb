//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_set.h"

namespace rocksdb {

class VersionEditHandler {
 public:
  explicit VersionEditHandler(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      VersionSet* version_set);
  virtual ~VersionEditHandler() {}
  Status Initialize();
  Status Iterate(log::Reader& reader, std::string* db_id);
  Status ApplyOneVersionEditToBuilder(VersionEdit& edit,
                                      ColumnFamilyData** cfd);
  virtual Status OnColumnFamilyAdd(VersionEdit& edit, ColumnFamilyData** cfd);
  virtual Status OnColumnFamilyDrop(VersionEdit& edit, ColumnFamilyData** cfd);
  virtual Status OnNonCfOperation(VersionEdit& edit, ColumnFamilyData** cfd);

  std::unordered_map<uint32_t, std::unique_ptr<BaseReferencedVersionBuilder>>&
  builders() {
    return builders_;
  }
  const std::string& GetDbId() const { return db_id_; }
  const std::unordered_map<uint32_t, std::string>& column_families_not_found()
      const {
    return column_families_not_found_;
  }
  const VersionEditParams& version_edit_params() const {
    return version_edit_params_;
  }

 protected:
  void CheckColumnFamilyId(const VersionEdit& edit, bool* cf_in_not_found,
                           bool* cf_in_builders) const;
  virtual ColumnFamilyData* CreateCfAndInit(
      const ColumnFamilyOptions& cf_options, VersionEdit* edit);
  const std::vector<ColumnFamilyDescriptor>& column_families_;
  VersionSet* version_set_;
  AtomicGroupReadBuffer read_buffer_;
  std::unordered_map<uint32_t, std::unique_ptr<BaseReferencedVersionBuilder>>
      builders_;
  std::string db_id_;
  std::unordered_map<std::string, ColumnFamilyOptions> name_to_options_;
  std::unordered_map<uint32_t, std::string> column_families_not_found_;
  VersionEditParams version_edit_params_;

 private:
  Status ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                    const VersionEdit& edit);
};

}  // namespace rocksdb
