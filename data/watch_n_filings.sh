#!/usr/bin/env bash
watch --interval 60 -x -d  mongo localhost:27020/edgar --eval "db.filings_13f.count()"



