package main

import (
	"errors"
	"fmt"

	json "github.com/goccy/go-json"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
	"github.com/spf13/cobra"
)

func newRepairCommand() *cobra.Command {
	repair := &cobra.Command{
		Use:   "repair",
		Short: "Repair offline storage artifacts",
	}
	repair.AddCommand(newRepairSegmentsCommand())
	return repair
}

func newRepairSegmentsCommand() *cobra.Command {
	var path string
	var dryRun bool
	var rebuildCorrupt bool
	cmd := &cobra.Command{
		Use:   "segments",
		Short: "Validate and repair segment log index files",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if path == "" {
				return oops.In("cli").Code("repair_path_required").Errorf("segment log path is required")
			}
			result, repairErr := store.RepairStorxLog(path, store.StorxLogRepairOptions{
				DryRun:                dryRun,
				RebuildCorruptIndexes: rebuildCorrupt,
			})
			return errors.Join(writeRepairResult(cmd, result), repairErr)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&path, "path", "", "segment log root path")
	flags.BoolVar(&dryRun, "dry-run", false, "report repairs without writing index files")
	flags.BoolVar(&rebuildCorrupt, "rebuild-corrupt-indexes", false, "move corrupt index files aside and rebuild them from segment files")
	return cmd
}

func writeRepairResult(cmd *cobra.Command, result store.StorxLogRepairResult) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return oops.In("cli").Code("repair_result_encode_failed").Wrapf(err, "encode repair result")
	}
	if _, err := fmt.Fprintln(cmd.OutOrStdout(), string(data)); err != nil {
		return oops.In("cli").Code("repair_result_write_failed").Wrapf(err, "write repair result")
	}
	return nil
}
