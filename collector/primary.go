// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// checkout table have `PRIMARY KEY`.?

package collector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// mysqlSubsystem used for metric names.
const primarySubsystem = "primary_key"

const primaryQuery = `
		  SELECT
			t.table_schema ,
			t.table_name ,
			t.engine 
		  FROM
    		information_schema.tables t
          LEFT JOIN
			information_schema.table_constraints tc
			ON t.table_schema = tc.table_schema
			AND t.table_name = tc.table_name
			AND tc.constraint_type = 'PRIMARY KEY'
          WHERE
			t.table_type = 'BASE TABLE'
			AND tc.constraint_type IS NULL
			AND t.table_schema!='performance_schema'
			AND t.table_schema!='mysql'
          ORDER BY
    		t.table_name;
		`

var (
	PrimarylabelNames = []string{"table_schema", "table_name", "engine"}
)

// Metric descriptors.
var (
	CheckPrimaryKeyDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, primarySubsystem, "check"),
		"Check table have `PRIMARY KEY`.",
		PrimarylabelNames, nil)
)

// ScrapeUser collects from `information_schema.processlist`.
type ScrapePrimaryKey struct{}

// Name of the Scraper. Should be unique.
func (ScrapePrimaryKey) Name() string {
	return primarySubsystem
}

// Help describes the role of the Scraper.
func (ScrapePrimaryKey) Help() string {
	return "Check table have `PRIMARY KEY`."
}

// Version of MySQL from which scraper is available.
func (ScrapePrimaryKey) Version() float64 {
	return 5.7
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePrimaryKey) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()

	primaryQuery := fmt.Sprint(primaryQuery)
	primaryRows, err := db.QueryContext(ctx, primaryQuery)
	if err != nil {
		return err
	}
	defer primaryRows.Close()

	var (
		table_schema string
		table_name   string
		engine       string
	)

	for primaryRows.Next() {
		err = primaryRows.Scan(
			&table_schema,
			&table_name,
			&engine,
		)

		if err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(CheckPrimaryKeyDesc, prometheus.GaugeValue, 0, table_schema, table_name, engine)
	}

	return nil
}

var _ Scraper = ScrapePrimaryKey{}
