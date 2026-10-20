import unittest
from align import validate


class AlignmentValidation(unittest.TestCase):
    def test_requires_every_word(self):
        with self.assertRaisesRegex(ValueError, "omitted"):
            validate("hello world", [{"word": "hello", "start": 0.1, "end": 0.3, "score": 0.8}], 1)

    def test_rejects_interpolated_words_without_scores(self):
        with self.assertRaisesRegex(ValueError, "lacks acoustic"):
            validate("hello", [{"word": "hello", "start": 0.1, "end": 0.3}], 1)

    def test_preserves_valid_acoustic_boundaries(self):
        result = validate("hello", [{"word": "hello", "start": 0.1, "end": 0.3, "score": 0.8}], 1)
        self.assertEqual(result, [{"text": "hello", "start": 0.1, "end": 0.3, "score": 0.8}])

    def test_rejects_nonfinite_or_out_of_range(self):
        for start, end, score in [(float("nan"), .3, .8), (.1, 2, .8), (.3, .1, .8), (.1, .3, 2)]:
            with self.assertRaises(ValueError):
                validate("hello", [{"word": "hello", "start": start, "end": end, "score": score}], 1)


if __name__ == "__main__":
    unittest.main()
