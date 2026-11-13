package config

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Validator can be implemented by configuration structs to run custom validation
// automatically at the end of Load / LoadFrom.
type Validator interface {
	Validate() error
}

// GetEnv returns the value of the environment variable named by key, or
// defaultVal if the variable is not present or empty.
func GetEnv(key, defaultVal string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return defaultVal
}

// RequireEnv returns the value of a non-empty environment variable or an error.
func RequireEnv(key string) (string, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("config: required environment variable %q is not set", key)
	}
	return v, nil
}

// MustGetEnv returns the value of a non-empty environment variable or panics.
func MustGetEnv(key string) string {
	v, err := RequireEnv(key)
	if err != nil {
		panic(err.Error())
	}
	return v
}

// GetEnvInt reads an integer environment variable or returns defaultVal if unset/empty.
func GetEnvInt(key string, defaultVal int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return defaultVal, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("config: invalid int for %q (%q): %w", key, v, err)
	}
	return n, nil
}

// GetEnvInt64 reads an int64 environment variable or returns defaultVal if unset/empty.
func GetEnvInt64(key string, defaultVal int64) (int64, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return defaultVal, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: invalid int64 for %q (%q): %w", key, v, err)
	}
	return n, nil
}

// GetEnvDuration reads a time.Duration environment variable or returns defaultVal if unset/empty.
func GetEnvDuration(key string, defaultVal time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("config: invalid duration for %q (%q): %w", key, v, err)
	}
	return d, nil
}

// GetEnvBool reads a boolean environment variable or returns defaultVal if unset/empty.
func GetEnvBool(key string, defaultVal bool) (bool, error) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return defaultVal, nil
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return false, fmt.Errorf("config: invalid bool for %q (%q): %w", key, v, err)
	}
	return b, nil
}

// Load populates target (a non-nil pointer to a struct) from process environment variables.
// Supported struct tags:
//   - `env:"VAR_NAME"`: environment variable name
//   - `default:"val"`: fallback value when the env var is unset or empty
//   - `required:"true"`: fails if unset/empty and no default is provided
//
// If target implements Validator, Validate() is called after loading all fields.
func Load(target any) error {
	return LoadFrom(target, os.LookupEnv)
}

// LoadFrom populates target using a custom lookup function (useful for tests).
func LoadFrom(target any, lookup func(string) (string, bool)) error {
	if target == nil {
		return errors.New("config: target must not be nil")
	}
	if lookup == nil {
		return errors.New("config: lookup function must not be nil")
	}
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Pointer || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return errors.New("config: target must be a non-nil pointer to a struct")
	}

	if err := loadStruct(rv.Elem(), lookup); err != nil {
		return err
	}

	if v, ok := target.(Validator); ok {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("config: validation failed: %w", err)
		}
	}
	return nil
}

func loadStruct(sv reflect.Value, lookup func(string) (string, bool)) error {
	st := sv.Type()
	var errs []error

	for i := 0; i < st.NumField(); i++ {
		sf := st.Field(i)
		fv := sv.Field(i)
		if !fv.CanSet() {
			continue
		}

		// Recurse into nested structs (unless it has an explicit env tag or is time.Duration, which is int64).
		if fv.Kind() == reflect.Struct && sf.Tag.Get("env") == "" {
			if err := loadStruct(fv, lookup); err != nil {
				errs = append(errs, err)
			}
			continue
		}

		envKey := sf.Tag.Get("env")
		if envKey == "" {
			continue
		}

		rawVal, ok := lookup(envKey)
		if !ok || rawVal == "" {
			if def, hasDef := sf.Tag.Lookup("default"); hasDef {
				rawVal = def
				ok = true
			}
		}

		required := strings.EqualFold(sf.Tag.Get("required"), "true")
		if (!ok || strings.TrimSpace(rawVal) == "") && required {
			errs = append(errs, fmt.Errorf("config: required env var %q (field %s) is not set", envKey, sf.Name))
			continue
		}

		if !ok || rawVal == "" {
			continue
		}

		if err := setField(fv, envKey, rawVal); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

var durationType = reflect.TypeOf(time.Duration(0))

func setField(fv reflect.Value, envKey, rawVal string) error {
	trimmed := strings.TrimSpace(rawVal)

	if fv.Type() == durationType {
		d, err := time.ParseDuration(trimmed)
		if err != nil {
			return fmt.Errorf("config: invalid duration for %q (%q): %w", envKey, rawVal, err)
		}
		fv.SetInt(int64(d))
		return nil
	}

	switch fv.Kind() {
	case reflect.String:
		fv.SetString(rawVal)
	case reflect.Bool:
		b, err := strconv.ParseBool(trimmed)
		if err != nil {
			return fmt.Errorf("config: invalid bool for %q (%q): %w", envKey, rawVal, err)
		}
		fv.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(trimmed, 10, fv.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: invalid integer for %q (%q): %w", envKey, rawVal, err)
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(trimmed, 10, fv.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: invalid unsigned integer for %q (%q): %w", envKey, rawVal, err)
		}
		fv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(trimmed, fv.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: invalid float for %q (%q): %w", envKey, rawVal, err)
		}
		fv.SetFloat(f)
	case reflect.Slice:
		if fv.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("config: unsupported slice element type %s for %q", fv.Type().Elem(), envKey)
		}
		parts := strings.Split(rawVal, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if s := strings.TrimSpace(p); s != "" {
				out = append(out, s)
			}
		}
		fv.Set(reflect.ValueOf(out))
	default:
		return fmt.Errorf("config: unsupported field type %s for %q", fv.Type(), envKey)
	}
	return nil
}
