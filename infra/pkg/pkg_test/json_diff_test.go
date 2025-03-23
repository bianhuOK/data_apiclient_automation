package json_diff

import (
	"encoding/json"
	"testing"

	jd "github.com/josephburnett/jd/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONDiff(t *testing.T) {
	t.Run("测试用例1: 简单map比较", func(t *testing.T) {
		testCase1A := map[string]interface{}{
			"age":  25,
			"name": "test",
		}
		testCase1B := map[string]interface{}{
			"name": "test",
			"age":  25,
		}
		assert.Equal(t, true, jsonDiff(t, testCase1A, testCase1B, true))
	})

	t.Run("测试用例2: slice有序比较", func(t *testing.T) {
		testCase2A := []interface{}{
			"apple",
			"banana",
			"orange",
		}
		testCase2B := []interface{}{
			"apple",
			"orange",
			"banana",
		}
		assert.Equal(t, true, jsonDiff(t, testCase2A, testCase2B, true))
	})

	t.Run("测试用例3: slice无序比较", func(t *testing.T) {
		testCase3A := []interface{}{
			"apple",
			"banana",
			"orange",
		}
		testCase3B := []interface{}{
			"apple",
			"orange",
			"banana",
		}
		assert.Equal(t, true, jsonDiff(t, testCase3A, testCase3B, false))
	})

	t.Run("测试用例4: 嵌套结构比较", func(t *testing.T) {
		testCase4A := map[string]interface{}{
			"user": map[string]interface{}{
				"name": "John",
				"address": map[string]interface{}{
					"city": "New York",
					"zip":  10001,
				},
				"hobbies": []interface{}{
					map[string]interface{}{
						"name":  "reading",
						"years": 5,
					},
					map[string]interface{}{
						"name":  "gaming",
						"years": 3,
					},
				},
			},
		}

		testCase4B := map[string]interface{}{
			"user": map[string]interface{}{
				"name": "John",
				"address": map[string]interface{}{
					"city": "New York",
					"zip":  10002, // Different zip code
				},
				"hobbies": []interface{}{
					map[string]interface{}{
						"name":  "gaming",
						"years": 3,
					},
					map[string]interface{}{
						"name":  "reading",
						"years": 5,
					},
				},
			},
		}

		// Test ordered comparison (should fail due to hobbies order and zip)
		assert.Equal(t, false, jsonDiff(t, testCase4A, testCase4B, true))

		// Test unordered comparison (should still fail due to zip difference)
		assert.Equal(t, false, jsonDiff(t, testCase4A, testCase4B, false))
	})

	t.Run("测试用例5: 不同JSON类型比较", func(t *testing.T) {
		testCase5A := map[string]interface{}{
			"string_val": "test",
			"int_val":    42,
			"float_val":  3.14,
			"bool_val":   true,
			"null_val":   nil,
			"array_val":  []interface{}{1, "two", 3.0, true, nil},
			"object_val": map[string]interface{}{
				"nested": "value",
			},
		}

		testCase5B := map[string]interface{}{
			"string_val": "test",
			"int_val":    42.0, // Same value but different type (float vs int)
			"float_val":  3.14,
			"bool_val":   true,
			"null_val":   nil,
			"array_val":  []interface{}{1.0, "two", 3, true, nil}, // Mixed number types
			"object_val": map[string]interface{}{
				"nested": "value",
			},
		}

		// Should be equal because JSON doesn't distinguish between int and float
		assert.Equal(t, true, jsonDiff(t, testCase5A, testCase5B, true))
	})
}

func jsonDiff(t *testing.T, a, b any, inOrder bool) bool {
	aa, _ := json.MarshalIndent(a, "", "  ")
	bb, _ := json.MarshalIndent(b, "", "  ")
	as, err := jd.ReadJsonString(string(aa))
	bs, err := jd.ReadJsonString(string(bb))
	require.NoError(t, err)

	var options []jd.Option
	if !inOrder {
		// SET option treats arrays as unordered sets
		options = append(options, jd.SET)
	}

	diff := as.Diff(bs, options...)
	diffString := diff.Render()
	t.Logf("diffString: %s", diffString)
	return as.Equals(bs, options...)
}
