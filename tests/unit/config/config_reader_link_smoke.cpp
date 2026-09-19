// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/api/content_store_builder.h>

int main() {
    yams::api::ContentStoreBuilder builder;
    builder.withCompression(false);
    return 0;
}
