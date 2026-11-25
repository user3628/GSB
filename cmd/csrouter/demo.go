package main

import (
	"flag"
	"fmt"

	"csrouter/internal/app"
)

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	root := fs.String("root", "data/demo", "demo data root")
	skipMedium := fs.Bool("skip-medium", false, "run only the 400-ticket tier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	a, err := app.New(*root)
	if err != nil {
		return err
	}
	defer a.Close()

	imported, err := a.GenerateAndImport(0)
	if err != nil {
		return err
	}
	validation, err := a.ValidateBundle()
	if err != nil {
		return err
	}
	if !validation.OK {
		return fmt.Errorf("bundle validation failed")
	}
	fmt.Printf("generated and imported %d tickets in %d shards\n", imported.ImportedTickets, imported.Shards)

	for _, tier := range []string{"small", "medium"} {
		if tier == "medium" && *skipMedium {
			continue
		}
		result, err := a.RunTier(tier, app.RunOptions{ClearStop: true})
		if err != nil {
			return err
		}
		if result.Report == nil || !result.Report.Reference.Pass {
			return fmt.Errorf("%s reference comparison failed", tier)
		}
		fmt.Printf("%s: strategy=%s report=%s reference_pass=%v\n",
			tier, result.Strategy.SelectedStrategy, result.Report.ReportFingerprint, result.Report.Reference.Pass)
	}
	fmt.Println("demo complete; use `csrouter serve --root " + *root + "` to inspect HTTP endpoints")
	return nil
}
