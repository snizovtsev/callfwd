#include "csv_reader.hpp"

extern "C" {
#if !defined(restrict)
#define restrict __restrict
#endif
#include <zsv.h>
}

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <cstdlib>

void ZsvReader::Open(const char *csv_path) {
    struct zsv_opts opts;

    pipe_ = fopen(csv_path, "r");
    CHECK(pipe_);
    // if (!pipe_)
    //     return arrow::Status::IOError("cannot open lrn data");

    memset(&opts, 0, sizeof(opts));
    opts.stream = pipe_;
    num_rows_ = 0;

    zsv_ = zsv_new(&opts);
    CHECK(zsv_);
    // if (!zsv_)
    //     return arrow::Status::OutOfMemory("zsv alloc failed");
}

void ZsvReader::Close() noexcept {
    if (zsv_) {
        /* XXX: status */ zsv_delete(zsv_);
        zsv_ = nullptr;
    }

    if (pipe_) {
        fclose(pipe_);
        pipe_ = nullptr;
    }
}

ZsvReader::~ZsvReader() noexcept {
    Close();
}

bool ZsvReader::NextRow() {
    if (/*unlikely*/zsv_next_row(zsv_) != zsv_status_row)
        return false;
    ++num_rows_;
    return true;
}

bool LRNReader::NextRow(LRNRow &row) {
    if (!ZsvReader::NextRow())
        return false;

    CHECK(zsv_cell_count(zsv_) == 2);

    struct zsv_cell cell = zsv_get_cell(zsv_, 0);
    char *end, *val = reinterpret_cast<char*>(cell.str);
    row.pn = strtoull(val, &end, 10);
    CHECK(end == val + cell.len);

    cell = zsv_get_cell(zsv_, 1);
    val = reinterpret_cast<char*>(cell.str);
    row.rn = strtoull(val, &end, 10);
    CHECK(end == val + cell.len);

    return true;
}

bool DNCReader::NextRow(DNCRow &row) {
    if (!ZsvReader::NextRow())
        return false;

    CHECK(zsv_cell_count(zsv_) == 2);
    struct zsv_cell cell = zsv_get_cell(zsv_, 0);
    char *end, *val = reinterpret_cast<char*>(cell.str);
    row.pn = strtoul(val, &end, 10);
    row.pn *= 10000000;
    CHECK(end == val + cell.len);

    cell = zsv_get_cell(zsv_, 1);
    val = reinterpret_cast<char*>(cell.str);
    row.pn += strtoul(val, &end, 10);
    CHECK(end == val + cell.len);

    return true;
}

bool DNOReader::NextRow(DNORow &row) {
    if (!ZsvReader::NextRow())
        return false;

    CHECK(zsv_cell_count(zsv_) == 2);
    struct zsv_cell cell = zsv_get_cell(zsv_, 0);
    char *end, *val = reinterpret_cast<char*>(cell.str);
    row.pn = strtoull(val, &end, 10);
    CHECK(end == val + cell.len);

    cell = zsv_get_cell(zsv_, 1);
    val = reinterpret_cast<char*>(cell.str);
    row.type = strtoul(val, &end, 10);
    CHECK(end == val + cell.len);
    CHECK(row.type >= 1 && row.type <= 8);

    return true;
}

PnJoinReader::PnJoinReader() {
    memset(&rowbuf_, 0, sizeof(rowbuf_));
}

PnJoinReader::~PnJoinReader() noexcept {
    Close();
}

void PnJoinReader::Close() {
    lrn.Close();
    dnc.Close();
    dno.Close();
}

bool PnJoinReader::NextRow(PnRecord &rec) {
    if (rowbuf_.lrn.pn == 0) {
        if (!lrn.NextRow(rowbuf_.lrn))
            rowbuf_.lrn.pn = UINT64_MAX;
    }
    if (rowbuf_.dnc.pn == 0) {
        if (!dnc.NextRow(rowbuf_.dnc))
            rowbuf_.dnc.pn = UINT64_MAX;
    }
    if (rowbuf_.dno.pn == 0) {
        if (!dno.NextRow(rowbuf_.dno))
            rowbuf_.dno.pn = UINT64_MAX;
    }

    // every row in buf are ready
    uint64_t next_pn = std::min({
        rowbuf_.lrn.pn,
        rowbuf_.dnc.pn,
        rowbuf_.dno.pn,
    });

    memset(&rec, 0, sizeof(rec));
    if (next_pn == UINT64_MAX)
        return false;

    CHECK(next_pn > 0);
    CHECK(next_pn < (1ull << 34));

    if (rowbuf_.lrn.pn == next_pn) {
        rec.lrn = rowbuf_.lrn;
        if (!lrn.NextRow(rowbuf_.lrn))
            rowbuf_.lrn.pn = UINT64_MAX;
        CHECK(rowbuf_.lrn.pn > next_pn);
    }

    if (rowbuf_.dnc.pn == next_pn) {
        rec.dnc = rowbuf_.dnc;
        if (!dnc.NextRow(rowbuf_.dnc))
            rowbuf_.dnc.pn = UINT64_MAX;
        CHECK(rowbuf_.dnc.pn > next_pn);
    }

    if (rowbuf_.dno.pn == next_pn) {
        rec.dno = rowbuf_.dno;
        if (!dno.NextRow(rowbuf_.dno))
            rowbuf_.dno.pn = UINT64_MAX;
        CHECK(rowbuf_.dno.pn > next_pn);
    }

    ++num_rows_;
    return true;
}
