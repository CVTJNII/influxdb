package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
)

const description = `
Convert a database shards from b1 or bz1 format to tsm1 format.

This tool will make backup any directory before conversion. It
is up to the end-user to delete the backup on the disk. Backups are
named by suffixing the database name with '.bak'. The backups will
be ignored by the system since they are not registered with the cluster.

To restore a backup, delete the tsm version, rename the backup and
restart the node.`

var dbs string

func init() {
	flag.StringVar(&dbs, "dbs", "", "Comma-delimited list of databases to convert. Default is convert all")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <data-path> \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s\n\n", description)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "no data directory specified\n")
		os.Exit(1)
	}
	dataPath := os.Args[1]

	// Determine the list of databases
	dbs, err := ioutil.ReadDir(dataPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to access data directory at %s: %s\n", dataPath, err.Error())
		os.Exit(1)
	}

	// Get the list of shards for conversion.
	var shards []*ShardInfo
	for _, db := range dbs {
		d := NewDatabase(filepath.Join(dataPath, db.Name()))
		shs, err := d.Shards()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to access shards for database %s: %s\n", d.Name(), err.Error())
			os.Exit(1)
		}
		shards = append(shards, shs...)
	}
	sort.Sort(ShardInfos(shards))

	// Display list of convertible shards.
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "Database\tRetention\tPath\tEngine\tSize")
	for _, si := range shards {
		fullPath := filepath.Join(dataPath, si.Database, si.Path)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n", si.Database, si.RetentionPolicy, fullPath, si.FormatAsString(), si.Size)
	}
	w.Flush()

	// Get confirmation from user.
	fmt.Printf("\nThese databases will be converted. Proceed? y/N: ")
	liner := bufio.NewReader(os.Stdin)
	yn, err := liner.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read response: %s", err.Error())
	}
	yn = strings.TrimRight(strings.ToLower(yn), "\n")
	if yn != "y" {
		fmt.Println("Conversion aborted.")
		os.Exit(1)
	}
	fmt.Println("Conversion starting....")

	// Backup each directory.
	for _, db := range dbs {
		dest := filepath.Join(dataPath, db.Name()+".bak")
		src := filepath.Join(dataPath, db.Name())

		if _, err := os.Stat(dest); !os.IsNotExist(err) {
			fmt.Printf("Backup of database %s already exists\n", db.Name())
			os.Exit(1)
		}

		err = copyDir(dest, src)
		if err != nil {
			fmt.Printf("Backup of database %s failed: %s\n", db.Name(), err.Error())
			os.Exit(1)
		}
		fmt.Printf("Database %s backed up to %s\n", db.Name(), dest)
	}
}

// copyDir copies the directory at src to dest. If dest does not exist it
// will be created. It is up to the caller to ensure the paths don't overlap.
func copyDir(dest, src string) error {
	copyFile := func(path string, info os.FileInfo, err error) error {
		_ = "breakpoint"
		// Strip the src from the path and replace with dest.
		toPath := strings.Replace(path, src, dest, 1)

		// Copy it.
		if info.IsDir() {
			if err := os.MkdirAll(toPath, info.Mode()); err != nil {
				return err
			}
		} else {
			err := func() error {
				in, err := os.Open(path)
				if err != nil {
					return err
				}
				defer in.Close()

				out, err := os.OpenFile(toPath, os.O_CREATE|os.O_WRONLY, info.Mode())
				if err != nil {
					return err
				}
				defer out.Close()

				_, err = io.Copy(out, in)
				return err
			}()
			if err != nil {
				return err
			}
		}
		return nil
	}

	return filepath.Walk(src, copyFile)
}
