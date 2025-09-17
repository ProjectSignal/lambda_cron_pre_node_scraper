import datetime
import re
from typing import Optional, Dict, Any, List

from config import config
from utils import get_logger


def map_rapidapi_to_standard(rapid_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps data from RapidAPI response to standard database format.
    Maintains compatibility with existing RapidAPI to standard format mapping.
    """
    if not rapid_data or not isinstance(rapid_data, dict):
        return {}
    
    linkedin_username = rapid_data.get('username', 'N/A')
    transformed = {}
    
    # --- Critical: Preserve LinkedIn Username ---
    # This is essential for node processing - always preserve the original username
    if linkedin_username and linkedin_username != 'N/A':
        transformed["linkedinUsername"] = linkedin_username
    
    # --- Basic Info (Keep these fields even if potentially null/empty from API) ---
    transformed["linkedinHeadline"] = rapid_data.get("headline")
    transformed["about"] = rapid_data.get("summary")
    transformed["currentLocation"] = rapid_data.get("geo", {}).get("full")
    transformed["avatarURL"] = rapid_data.get("profilePicture")
    transformed["apiScraped"] = True
    
    # --- Contacts ---
    linkedin_url = f"https://www.linkedin.com/in/{linkedin_username}" if linkedin_username else None
    transformed["contacts"] = {
        "email": None,
        "linkedin": linkedin_url,
        "twitter": None,
        "website": None
    }
    
    # Background Image (Only add if present)
    background_images = rapid_data.get("backgroundImage", [])
    if background_images:
        best_background = sorted(
            background_images, 
            key=lambda img: img.get('width', 0) * img.get('height', 0), 
            reverse=True
        )
        if best_background and best_background[0].get('url'):
            transformed["backgroundImage"] = best_background[0].get('url')
    
    # --- Experience (Conditional) ---
    experience_list = []
    for pos in rapid_data.get("position", []):
        exp_item = {
            "title": pos.get("title"),
            "companyName": pos.get("companyName"),
            "companyUrl": pos.get("companyURL"),
            "companyIndustry": pos.get("companyIndustry"),
            "location": pos.get("location"),
            "duration": format_duration(pos.get("start"), pos.get("end")),
            "description": pos.get("description"),
            "companyLogo": pos.get("companyLogo"),
            "companyUsername": pos.get("companyUsername"),
            "companyStaffCountRange": pos.get("companyStaffCountRange")
        }
        
        # Add employmentType only if it has a value
        emp_type = pos.get("employmentType")
        if emp_type:
            exp_item["employmentType"] = emp_type
        
        experience_list.append(exp_item)
    
    if experience_list:
        transformed["workExperience"] = experience_list
    
    # --- Education (Conditional) ---
    education_list = []
    for edu in rapid_data.get("educations", []):
        edu_item = {
            "school": edu.get("schoolName"),
            "schoolUrl": edu.get("url"),
            "schoolLogo": edu.get("logo", [{}])[0].get("url") if edu.get("logo") else None,
            "degree": edu.get("degree"),
            "field_of_study": edu.get("fieldOfStudy"),
            "dates": format_duration(edu.get("start"), edu.get("end")),
            "description": edu.get("description"),
            "activities": edu.get("activities"),
            "grade": edu.get("grade"),
        }
        education_list.append(edu_item)
    
    if education_list:
        transformed["education"] = education_list
    
    # --- Skills (Conditional) ---
    skills_list = [skill.get("name") for skill in rapid_data.get("skills", []) if skill.get("name")]
    if skills_list:
        transformed["skills"] = skills_list
    
    # --- Accomplishments (Conditional - build dict first) ---
    accomplishments_dict = {}
    
    # Certifications
    cert_list = []
    for cert in rapid_data.get("certifications", []):
        cert_item = {
            "certificateName": cert.get("name"),
            "certificateFrom": cert.get("authority"),
            "dateRange": format_date(cert.get("start")),
            "certificateLogo": cert.get("company", {}).get("logo"),
        }
        if cert_item.get("certificateName"):
            cert_list.append(cert_item)
    
    if cert_list:
        accomplishments_dict["Certifications"] = cert_list
    
    # Honors & Awards
    honors_list = []
    for honor in rapid_data.get("honors", []):
        issue_date_obj = honor.get("issuedOn")
        issue_date_str = format_date(issue_date_obj) if issue_date_obj else ""
        honor_item = {
            "title": honor.get("title"),
            "issuer": honor.get("issuer"),
            "issuerLogo": honor.get("issuerLogo"),
            "dateRange": issue_date_str,
            "description": honor.get("description")
        }
        if honor_item.get("title"):
            honors_list.append(honor_item)
    
    if honors_list:
        accomplishments_dict["Honors"] = honors_list
    
    # Only add accomplishments_dict if it's not empty
    if accomplishments_dict:
        transformed["accomplishments"] = accomplishments_dict
    
    # Remove keys with None values before returning
    return {k: v for k, v in transformed.items() if v is not None}


def map_scrapfly_to_standard(scrapfly_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps data from Scrapfly API response to standard database format.
    Future implementation for Scrapfly API integration.
    """
    # Future implementation - placeholder for now
    return {}


def map_proxycurl_to_standard(proxycurl_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps data from Proxycurl API response to standard database format.
    Future implementation for Proxycurl API integration.
    """
    # Future implementation - placeholder for now
    return {}


def format_date(date_obj: Optional[Dict[str, Any]]) -> str:
    """Formats the date object {year, month, day} into 'Mon YYYY' or 'YYYY'."""
    if not date_obj or not isinstance(date_obj, dict) or not date_obj.get("year"):
        return ""
    
    year = date_obj["year"]
    month = date_obj.get("month", 0)
    
    if year == 0:  # Handle 'Present' cases or invalid year
        return "Present"
    
    if month and 1 <= month <= 12:
        try:
            # Create a date object (day doesn't matter for month/year format)
            dt = datetime.datetime(year, month, 1)
            return dt.strftime("%b %Y")
        except ValueError:
            # Handle cases where month might be invalid despite check
            return str(year)
    else:
        return str(year)


def format_duration(start_obj: Optional[Dict[str, Any]], 
                   end_obj: Optional[Dict[str, Any]]) -> str:
    """Formats start and end dates into a duration string like 'Mon YYYY - Mon YYYY (X yrs Y mos)'."""
    start_str = format_date(start_obj)
    end_str = format_date(end_obj)
    
    if not start_str:
        return ""
    
    # Build date range part
    if end_str == "Present" or not end_str:
        date_range_part = f"{start_str} - Present"
    else:
        date_range_part = f"{start_str} - {end_str}"
    
    # Calculate duration if possible
    duration_str = ""
    if (start_obj and end_obj and 
        start_obj.get("year") and end_obj.get("year") and 
        start_obj["year"] != 0 and end_obj["year"] != 0):
        
        start_year = start_obj["year"]
        end_year = end_obj["year"]
        # Default to month 1 if month is missing or 0 for calculation
        start_month = start_obj.get("month", 1) if start_obj.get("month", 0) > 0 else 1
        end_month = end_obj.get("month", 1) if end_obj.get("month", 0) > 0 else 1
        
        # Ensure end date is after start date for calculation
        if (end_year > start_year) or (end_year == start_year and end_month >= start_month):
            months = (end_year - start_year) * 12 + (end_month - start_month) + 1
            if months <= 0:
                months = 1  # Min 1 month if dates are same month/year
            
            years_dur = months // 12
            months_dur = months % 12
            
            parts = []
            if years_dur > 0:
                parts.append(f"{years_dur} yr{'s' if years_dur > 1 else ''}")
            if months_dur > 0:
                parts.append(f"{months_dur} mo{'s' if months_dur > 1 else ''}")
            if not parts:  # Handle case like Jan 2022 - Jan 2022 -> 1 mo
                parts.append("1 mo")
            
            duration_str = f" ({', '.join(parts)})"
    
    return f"{date_range_part}{duration_str}"


def normalize_profile_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize and clean profile data fields."""
    normalized = data.copy()
    
    # Clean and normalize text fields
    text_fields = ["linkedinHeadline", "about", "currentLocation"]
    for field in text_fields:
        if field in normalized and normalized[field]:
            # Basic text cleaning
            text = str(normalized[field]).strip()
            # Remove excessive whitespace
            text = re.sub(r'\s+', ' ', text)
            normalized[field] = text
    
    # Normalize skills to ensure they're properly formatted
    if "skills" in normalized and isinstance(normalized["skills"], list):
        normalized["skills"] = [str(skill).strip() for skill in normalized["skills"] if skill and str(skill).strip()]
    
    return normalized


def validate_extracted_data(data: Dict[str, Any], min_required: Optional[int] = None) -> bool:
    """
    Validate if extracted LinkedIn node data has minimum required fields.
    Uses node-specific validation criteria optimized for LinkedIn profiles.
    """
    min_required = min_required or config.MIN_POPULATED_FIELDS_THRESHOLD
    key_fields = config.REQUIRED_FIELDS_FOR_VALIDATION
    
    # Count non-empty, non-None fields with node-specific validation
    valid_fields = 0
    populated_fields = []
    field_quality_scores = {}
    
    for field in key_fields:
        value = data.get(field)
        field_valid = False
        field_score = 0
        
        if field == "linkedinHeadline":
            if value and str(value).strip():
                field_valid = True
                field_score = len(str(value).split())  # More words = better headline
        
        elif field == "about":
            if value and str(value).strip():
                field_valid = True
                about_length = len(str(value).strip())
                field_score = min(10, about_length // 20)  # Score based on length
        
        elif field == "currentLocation":
            if value and str(value).strip():
                field_valid = True
                # Bonus for detailed location (city, state/country)
                field_score = 2 if ',' in str(value) else 1
        
        elif field == "workExperience":
            if isinstance(value, list) and len(value) > 0:
                field_valid = True
                # Score based on number and quality of experiences
                field_score = min(5, len(value))
                
                # Bonus for detailed experience entries
                detailed_count = 0
                for exp in value[:3]:  # Check first 3 entries
                    if (isinstance(exp, dict) and 
                        exp.get('title') and exp.get('companyName')):
                        detailed_count += 1
                        if exp.get('description') or exp.get('duration'):
                            detailed_count += 0.5
                
                field_score += int(detailed_count)
        
        elif field == "education":
            if isinstance(value, list) and len(value) > 0:
                field_valid = True
                field_score = min(3, len(value))
                
                # Bonus for detailed education entries
                for edu in value[:2]:  # Check first 2 entries
                    if (isinstance(edu, dict) and 
                        edu.get('school') and edu.get('degree')):
                        field_score += 1
        
        elif field == "skills":
            if isinstance(value, list) and len(value) > 0:
                field_valid = True
                # Score based on number of skills
                skill_count = len(value)
                if skill_count >= 10:
                    field_score = 5
                elif skill_count >= 5:
                    field_score = 3
                else:
                    field_score = 1
        
        elif field == "avatarURL":
            if value and str(value).strip() and 'http' in str(value):
                field_valid = True
                field_score = 1
        
        elif field == "contacts":
            if isinstance(value, dict) and value:
                # Validate that contacts contains useful information
                contact_count = sum(1 for k, v in value.items() if v and str(v).strip())
                if contact_count > 0:
                    field_valid = True
                    field_score = contact_count
        
        else:
            # Generic validation for other fields
            if value and str(value).strip():
                field_valid = True
                field_score = 1
        
        if field_valid:
            valid_fields += 1
            populated_fields.append(field)
            field_quality_scores[field] = field_score
    
    # Calculate overall validation score
    total_quality_score = sum(field_quality_scores.values())
    
    # Enhanced validation logic
    is_valid = valid_fields >= min_required
    
    # Additional node-specific validation rules
    critical_fields_present = 0
    
    # Must have at least headline OR about section
    if data.get('linkedinHeadline') or data.get('about'):
        critical_fields_present += 1
    
    # Must have either work experience OR education
    if (data.get('workExperience') and len(data.get('workExperience', [])) > 0) or \
       (data.get('education') and len(data.get('education', [])) > 0):
        critical_fields_present += 1
    
    # Enhanced validation: require at least 2 critical field groups
    enhanced_validation = critical_fields_present >= 2 and total_quality_score >= 5
    
    # Final validation combines basic count and enhanced criteria
    final_validation = is_valid and enhanced_validation
    
    logger = get_logger(__name__)
    if final_validation:
        logger.info(f"Node data validation passed: {valid_fields}/{len(key_fields)} key fields present (required: {min_required})")
        logger.info(f"Quality score: {total_quality_score}, Critical field groups: {critical_fields_present}/2")
        logger.debug(f"Populated fields: {', '.join(populated_fields)}")
        logger.debug(f"Field quality scores: {field_quality_scores}")
    else:
        logger.warning(f"Node data validation failed: {valid_fields}/{len(key_fields)} key fields present (minimum required: {min_required})")
        logger.warning(f"Quality score: {total_quality_score}, Critical field groups: {critical_fields_present}/2")
        logger.debug(f"Populated fields: {', '.join(populated_fields) if populated_fields else 'None'}")
        
        missing_fields = []
        for f in key_fields:
            if f not in populated_fields:
                missing_fields.append(f)
        if missing_fields:
            logger.debug(f"Missing/empty fields: {', '.join(missing_fields)}")
    
    return final_validation


def calculate_quality_score(data: Dict[str, Any], provider: str) -> int:
    """
    Calculate a node-specific quality score (0-100) based on LinkedIn profile data completeness and richness.
    
    Scoring breakdown for LinkedIn node data:
    - Critical fields (60 points): headline, about, experience, education
    - Important fields (25 points): skills, location, avatar, contacts
    - Enhanced fields (15 points): accomplishments, background image, detailed field content
    """
    if not data:
        return 0
    
    score = 0
    
    # === Critical Fields (60 points total) ===
    # LinkedIn Headline (15 points)
    headline = data.get('linkedinHeadline', '')
    if headline and str(headline).strip():
        score += 15
        # Bonus for detailed headline (3+ words)
        if len(str(headline).split()) >= 3:
            score += 2
    
    # About/Summary Section (15 points)
    about = data.get('about', '')
    if about and str(about).strip():
        about_length = len(str(about).strip())
        if about_length > 0:
            score += 10
            # Bonus points for substantial about section
            if about_length > 100:
                score += 3
            if about_length > 300:
                score += 2
    
    # Work Experience (20 points)
    work_exp = data.get('workExperience', [])
    if isinstance(work_exp, list) and len(work_exp) > 0:
        base_exp_score = 12  # Base for having experience
        score += base_exp_score
        
        # Additional points for multiple experiences (up to 8 points)
        exp_count = len(work_exp)
        if exp_count >= 2:
            score += 3
        if exp_count >= 3:
            score += 3
        if exp_count >= 5:
            score += 2
        
        # Bonus for detailed experience entries
        detailed_experiences = 0
        for exp in work_exp[:3]:  # Check first 3 experiences
            if (isinstance(exp, dict) and 
                exp.get('title') and exp.get('companyName') and 
                (exp.get('description') or exp.get('duration'))):
                detailed_experiences += 1
        
        if detailed_experiences > 0:
            score += min(3, detailed_experiences)  # Up to 3 bonus points
    
    # Education (10 points)
    education = data.get('education', [])
    if isinstance(education, list) and len(education) > 0:
        score += 8
        # Bonus for multiple education entries
        if len(education) >= 2:
            score += 2
    
    # === Important Fields (25 points total) ===
    # Skills (8 points)
    skills = data.get('skills', [])
    if isinstance(skills, list) and len(skills) > 0:
        skill_count = len(skills)
        if skill_count > 0:
            score += 5
            # Bonus for substantial skills list
            if skill_count >= 5:
                score += 2
            if skill_count >= 10:
                score += 1
    
    # Current Location (4 points)
    location = data.get('currentLocation', '')
    if location and str(location).strip():
        score += 4
    
    # Profile Avatar (4 points)
    avatar = data.get('avatarURL', '')
    if avatar and str(avatar).strip() and 'http' in str(avatar):
        score += 4
    
    # Contact Information (5 points)
    contacts = data.get('contacts', {})
    if isinstance(contacts, dict):
        contact_score = 0
        if contacts.get('linkedin'):
            contact_score += 3
        if contacts.get('email'):
            contact_score += 1
        if contacts.get('website'):
            contact_score += 1
        score += min(5, contact_score)
    
    # LinkedIn Username preservation (4 points)
    if data.get('linkedinUsername') or (contacts and contacts.get('linkedin')):
        score += 4
    
    # === Enhanced Fields (15 points total) ===
    # Accomplishments/Certifications (6 points)
    accomplishments = data.get('accomplishments', {})
    if isinstance(accomplishments, dict) and accomplishments:
        acc_score = 0
        if accomplishments.get('Certifications'):
            acc_score += 3
        if accomplishments.get('Honors'):
            acc_score += 2
        if len(accomplishments.keys()) >= 3:
            acc_score += 1
        score += min(6, acc_score)
    
    # Background Image (3 points)
    background = data.get('backgroundImage', '')
    if background and str(background).strip() and 'http' in str(background):
        score += 3
    
    # Data Processing Quality (6 points)
    # Bonus for successful API scraping flag
    if data.get('apiScraped') is True:
        score += 2
    
    # Bonus for data transformation metadata
    if data.get('processed_via'):
        score += 1
    
    # Bonus for quality validation
    if data.get('data_validation_passed') is True:
        score += 1
    
    # Bonus for having processing timestamps
    if data.get('processedAt') or data.get('extractedAt'):
        score += 1
    
    # Provider-specific bonuses
    if provider == "rapidapi":
        # RapidAPI typically provides rich data structure
        if (work_exp and len(work_exp) > 0 and 
            isinstance(work_exp[0], dict) and 
            work_exp[0].get('companyUrl')):
            score += 1
    elif provider == "scrapfly":
        # Scrapfly might provide enhanced HTML parsing
        if about and len(str(about)) > 200:
            score += 1
    elif provider == "proxycurl":
        # Proxycurl often provides structured contact data
        if contacts and len(contacts.keys()) > 2:
            score += 1
    
    return min(score, 100)


def add_processing_metadata(data: Dict[str, Any], provider: str, 
                          quality_score: Optional[int] = None) -> Dict[str, Any]:
    """Add metadata fields for processing tracking with enhanced information."""
    now = datetime.datetime.now(datetime.timezone.utc)
    
    if quality_score is None:
        quality_score = calculate_quality_score(data, provider)
    
    timestamp_iso = now.isoformat()

    metadata = {
        "platform": config.PLATFORM,
        "scrapped": True,
        "extracted": True,
        "apiScraped": True,
        "processed_via": provider,
        "extractedAt": timestamp_iso,
        "scrappedAt": timestamp_iso,
        "processedAt": timestamp_iso,
        "processor_version": config.PROCESSOR_VERSION,
        "quality_score": quality_score,
        "data_validation_passed": validate_extracted_data(data)
    }
    
    # Add provider-specific metadata
    if provider == "rapidapi":
        metadata["extraction_method"] = "rapidapi_direct"
    elif provider == "scrapfly":
        metadata["extraction_method"] = "scrapfly_api"
    elif provider == "proxycurl":
        metadata["extraction_method"] = "proxycurl_api"
    
    # Merge with existing data, ensuring metadata doesn't override important data
    result = {**data, **metadata}
    
    # Remove None values
    return {k: v for k, v in result.items() if v is not None}


def validate_provider_data(data: Dict[str, Any], provider: str) -> Dict[str, Any]:
    """
    Validate and report on data quality for normalized profile payloads.

    Args:
        data: Profile document using standard fields such as linkedinHeadline,
            about, workExperience, education, etc. (i.e. post-transformation data).
        provider: Name of the upstream provider for logging/metrics only.

    Returns:
        Structured quality report containing the derived score and validity flag.
    """
    logger = get_logger(__name__)

    if not data:
        return {
            "valid": False, 
            "quality_score": 0, 
            "quality_report": "No data provided",
            "data": None,
            "provider": provider
        }
    
    # Calculate quality score (reuse existing score if already present)
    quality_score = data.get("quality_score")
    if isinstance(quality_score, str) and quality_score.isdigit():
        quality_score = int(quality_score)
    if not isinstance(quality_score, (int, float)):
        quality_score = calculate_quality_score(data, provider)
    else:
        quality_score = int(quality_score)
    
    quality_metrics = {
        "total_fields": len(data),
        "populated_fields": len([v for v in data.values() if v and str(v).strip()]),
        "critical_fields_present": 0,
        "provider": provider,
        "quality_score": quality_score
    }
    
    # Check critical fields
    critical_fields = ["linkedinHeadline", "about", "workExperience"]
    for field in critical_fields:
        if data.get(field):
            if field in ["workExperience", "education"] and isinstance(data[field], list):
                if len(data[field]) > 0:
                    quality_metrics["critical_fields_present"] += 1
            elif str(data[field]).strip():
                quality_metrics["critical_fields_present"] += 1
    
    # Validate using existing validation function
    is_valid = validate_extracted_data(data)
    
    quality_report = f"Provider: {provider}, Score: {quality_score}/100, " \
                    f"Fields: {quality_metrics['populated_fields']}/{quality_metrics['total_fields']}, " \
                    f"Critical: {quality_metrics['critical_fields_present']}/{len(critical_fields)}"
    
    logger.info(quality_report)
    
    return {
        "valid": is_valid,
        "quality_score": quality_score,
        "quality_report": quality_report,
        "quality_metrics": quality_metrics,
        "data": data,
        "provider": provider
    }


def merge_provider_data(primary_data: Dict[str, Any], fallback_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge data from multiple providers, giving priority to primary provider
    but filling in gaps with fallback data.
    """
    if not primary_data:
        return fallback_data or {}
    
    if not fallback_data:
        return primary_data
    
    merged = fallback_data.copy()  # Start with fallback as base
    
    # Override with primary data (primary takes precedence)
    for key, value in primary_data.items():
        if value and str(value).strip():  # Only override with non-empty values
            merged[key] = value
    
    return merged


class DataTransformer:
    """Enterprise-grade data transformation logic for profile data with provider-specific mapping"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def transform_data(self, raw_data: Dict[str, Any], provider: str = "rapidapi") -> Optional[Dict[str, Any]]:
        """
        Transform raw API data to standard format based on provider.
        Returns enhanced data with quality metrics and metadata.
        """
        if not raw_data or not isinstance(raw_data, dict):
            self.logger.error("Invalid or empty data received for transformation")
            return None
        
        linkedin_username = raw_data.get('username', 'N/A')
        self.logger.info(f"Transforming data for {linkedin_username} from provider: {provider}")
        
        # Apply provider-specific mapping
        if provider == "rapidapi":
            transformed_data = map_rapidapi_to_standard(raw_data)
        elif provider == "scrapfly":
            transformed_data = map_scrapfly_to_standard(raw_data)
        elif provider == "proxycurl":
            transformed_data = map_proxycurl_to_standard(raw_data)
        else:
            self.logger.error(f"Unknown provider for transformation: {provider}")
            return None
        
        if not transformed_data:
            self.logger.error(f"Provider-specific transformation failed for {provider}")
            return None
        
        # Normalize the data
        normalized_data = normalize_profile_data(transformed_data)
        
        # Calculate quality score
        quality_score = calculate_quality_score(normalized_data, provider)
        
        # Add processing metadata
        final_data = add_processing_metadata(normalized_data, provider, quality_score)
        
        # Validate the final result
        if not self.validate_transformed_data(final_data):
            self.logger.warning(f"Final data validation failed for {linkedin_username}")
        
        self.logger.info(f"Successfully transformed data for {linkedin_username} (Quality Score: {quality_score})")
        return final_data
    
    def validate_transformed_data(self, data: Dict[str, Any]) -> bool:
        """Validate that transformed data meets expected schema requirements"""
        try:
            # Basic validation - ensure required fields are present
            if not isinstance(data, dict):
                return False
            
            # Check that apiScraped is properly set
            if not data.get("apiScraped"):
                self.logger.warning("Transformed data missing apiScraped flag")
                return False
            
            # Validate contacts structure if present
            if "contacts" in data:
                contacts = data["contacts"]
                if not isinstance(contacts, dict):
                    self.logger.warning("Contacts field is not a dict")
                    return False
            
            # Validate work experience structure if present
            if "workExperience" in data:
                if not isinstance(data["workExperience"], list):
                    self.logger.warning("Work experience is not a list")
                    return False
            
            # Validate education structure if present
            if "education" in data:
                if not isinstance(data["education"], list):
                    self.logger.warning("Education is not a list")
                    return False
            
            # Validate skills structure if present
            if "skills" in data:
                if not isinstance(data["skills"], list):
                    self.logger.warning("Skills is not a list")
                    return False
            
            # Validate accomplishments structure if present
            if "accomplishments" in data:
                if not isinstance(data["accomplishments"], dict):
                    self.logger.warning("Accomplishments is not a dict")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating transformed data: {e}")
            return False
