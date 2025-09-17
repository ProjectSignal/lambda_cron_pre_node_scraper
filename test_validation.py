import os
import unittest

# Ensure required environment variables exist before importing config-dependent modules
os.environ.setdefault("BASE_API_URL", "https://example.test")
os.environ.setdefault("INSIGHTS_API_KEY", "test-key")
os.environ.setdefault("RAPIDAPI_KEY", "dummy-key")
os.environ.setdefault("RAPIDAPI_HOST", "example.test")

from data_transformer import DataTransformer, validate_provider_data
from config import config


class RapidAPIValidationTest(unittest.TestCase):
    def setUp(self):
        self.transformer = DataTransformer()
        self.sample_profile = {
            "username": "design-leader",
            "headline": "Global Product Design Leader",
            "summary": (
                "Seasoned design executive with a track record of building award-winning "
                "products and teams across multiple industries. Passionate about human-"
                "centered design, scaling design operations, and delivering business impact. "
                "Mentors emerging designers and frequently speaks at international design "
                "conferences about the future of product storytelling and inclusive design."
            ),
            "geo": {"full": "San Francisco, California, United States"},
            "profilePicture": "https://example.test/avatar.jpg",
            "backgroundImage": [
                {"url": "https://example.test/background.jpg", "width": 1600, "height": 900}
            ],
            "position": [
                {
                    "title": "Head of Design",
                    "companyName": "Visionary Labs",
                    "companyURL": "https://visionarylabs.test",
                    "companyIndustry": "Design Services",
                    "location": "San Francisco Bay Area",
                    "start": {"year": 2019, "month": 3, "day": 0},
                    "end": {"year": 0, "month": 0, "day": 0},
                    "description": "Leading 40+ designers to ship platform experiences.",
                    "companyLogo": "https://example.test/visionary.png",
                    "companyUsername": "visionary-labs",
                    "companyStaffCountRange": "51 - 200",
                },
                {
                    "title": "Design Director",
                    "companyName": "Northstar Apps",
                    "companyURL": "https://northstar.test",
                    "companyIndustry": "Computer Software",
                    "location": "Seattle, Washington",
                    "start": {"year": 2015, "month": 6, "day": 0},
                    "end": {"year": 2019, "month": 2, "day": 0},
                    "description": "Shipped growth-driving consumer app experiences.",
                    "companyLogo": "https://example.test/northstar.png",
                    "companyUsername": "northstar-apps",
                    "companyStaffCountRange": "201 - 500",
                },
                {
                    "title": "Senior Product Designer",
                    "companyName": "Create.io",
                    "companyIndustry": "Internet",
                    "location": "Austin, Texas",
                    "start": {"year": 2011, "month": 1, "day": 0},
                    "end": {"year": 2015, "month": 5, "day": 0},
                    "description": "Owned design for collaboration tools used by 5M+ users.",
                    "companyLogo": "https://example.test/create.png",
                    "companyUsername": "create-io",
                    "companyStaffCountRange": "51 - 200",
                },
            ],
            "educations": [
                {
                    "schoolName": "Stanford University",
                    "url": "https://stanford.test",
                    "logo": [{"url": "https://example.test/stanford.png"}],
                    "degree": "MS",
                    "fieldOfStudy": "Design Impact",
                    "start": {"year": 2009, "month": 9, "day": 0},
                    "end": {"year": 2011, "month": 6, "day": 0},
                    "description": "Graduate research in design leadership.",
                    "activities": "Teaching Assistant, Design for Extreme Affordability",
                    "grade": "4.0",
                },
                {
                    "schoolName": "Rhode Island School of Design",
                    "degree": "BFA",
                    "fieldOfStudy": "Industrial Design",
                    "start": {"year": 2005, "month": 9, "day": 0},
                    "end": {"year": 2009, "month": 6, "day": 0},
                    "description": "Studio lead for collaborative design projects.",
                    "activities": "IDSA Student Chapter President",
                    "grade": "3.8",
                },
            ],
            "skills": [
                {"name": "Product Strategy"},
                {"name": "Design Systems"},
                {"name": "Leadership"},
                {"name": "UX Research"},
                {"name": "Interaction Design"},
                {"name": "Prototyping"},
                {"name": "Design Thinking"},
                {"name": "Inclusive Design"},
                {"name": "Team Building"},
                {"name": "Storytelling"},
                {"name": "Workshop Facilitation"},
            ],
            "certifications": [
                {
                    "name": "Certified Design Leader",
                    "authority": "International Design Board",
                    "start": {"year": 2017, "month": 4, "day": 0},
                    "company": {"logo": "https://example.test/cert.png"},
                }
            ],
            "honors": [
                {
                    "title": "Top 50 Designers",
                    "issuer": "Design Magazine",
                    "issuedOn": {"year": 2020, "month": 5, "day": 0},
                    "issuerLogo": "https://example.test/honor.png",
                    "description": "Recognized for industry impact.",
                }
            ],
        }

    def test_transformed_data_passes_validation_threshold(self):
        transformed = self.transformer.transform_data(self.sample_profile, "rapidapi")
        self.assertIsNotNone(transformed, "Transformation should yield profile data")

        validation = validate_provider_data(transformed, "rapidapi")
        self.assertTrue(validation["valid"], validation["quality_report"])
        self.assertGreaterEqual(
            validation["quality_score"],
            config.QUALITY_SCORE_THRESHOLD,
            "Quality score should satisfy configured threshold",
        )


if __name__ == "__main__":
    unittest.main()
