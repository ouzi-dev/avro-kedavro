package cmd

import (
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
	"github.com/ouzi-dev/avro-kedavro/pkg/kedavro"
	"github.com/spf13/cobra"
)

var schemaFilePath = ""

// verifySchemaCmd represents the verifySchema command
var verifySchemaCmd = &cobra.Command{
	Use:   "verifySchema",
	Short: "Verifies an avro schema is syntactically correct",
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := ioutil.ReadFile(schemaFilePath)
		if err != nil {
			// Error reading file
			return err
		}

		_, err = kedavro.NewParser(string(data), kedavro.WithStringToNumber(), kedavro.WithTimestampToMillis())
		if err != nil {
			// Error parsing schema
			return err
		}

		_, err = goavro.NewCodec(string(data))
		if err != nil {
			// Error building a codec.
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(verifySchemaCmd)

	verifySchemaCmd.Flags().StringVarP(&schemaFilePath, "schema-file-path", "s", "", "The path to the schema file to check")
	_ = verifySchemaCmd.MarkFlagRequired("schema-file-path")
}
