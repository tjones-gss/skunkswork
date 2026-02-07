#!/usr/bin/env python3
"""
Database Initialization Script
NAM Competitive Intelligence Pipeline

Creates all required tables and indexes.
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()

# SQL for creating tables
CREATE_TABLES_SQL = """
-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy text search

-- =============================================================================
-- COMPANIES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS companies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Core identification
    canonical_name VARCHAR(500) NOT NULL,
    normalized_name VARCHAR(500) NOT NULL,
    domain VARCHAR(255),
    website VARCHAR(500),
    
    -- Location
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100) DEFAULT 'United States',
    full_address TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Firmographics
    employee_count_min INTEGER,
    employee_count_max INTEGER,
    revenue_min_usd BIGINT,
    revenue_max_usd BIGINT,
    year_founded INTEGER,
    naics_code VARCHAR(10),
    sic_code VARCHAR(10),
    industry VARCHAR(200),
    
    -- Technology
    erp_system VARCHAR(100),
    crm_system VARCHAR(100),
    tech_stack JSONB DEFAULT '[]'::jsonb,
    
    -- Metadata
    quality_score INTEGER CHECK (quality_score >= 0 AND quality_score <= 100),
    quality_grade CHAR(1),
    data_sources JSONB DEFAULT '[]'::jsonb,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_verified_at TIMESTAMP WITH TIME ZONE,
    
    -- Constraints
    CONSTRAINT unique_domain UNIQUE (domain)
);

-- Indexes for companies
CREATE INDEX IF NOT EXISTS idx_companies_name_trgm ON companies USING gin(canonical_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_companies_normalized_name ON companies (normalized_name);
CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies (domain);
CREATE INDEX IF NOT EXISTS idx_companies_location ON companies (state, city);
CREATE INDEX IF NOT EXISTS idx_companies_quality ON companies (quality_score DESC);
CREATE INDEX IF NOT EXISTS idx_companies_erp ON companies (erp_system);
CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies (industry);
CREATE INDEX IF NOT EXISTS idx_companies_employee_count ON companies (employee_count_min, employee_count_max);

-- =============================================================================
-- ASSOCIATION MEMBERSHIPS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS association_memberships (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Association info
    association_code VARCHAR(20) NOT NULL,
    association_name VARCHAR(200),
    
    -- Membership details
    membership_tier VARCHAR(50),
    membership_status VARCHAR(20) DEFAULT 'active',
    member_since INTEGER,  -- Year
    
    -- Source
    profile_url VARCHAR(500),
    raw_data JSONB,
    
    -- Timestamps
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_membership UNIQUE (company_id, association_code)
);

-- Indexes for memberships
CREATE INDEX IF NOT EXISTS idx_memberships_association ON association_memberships (association_code);
CREATE INDEX IF NOT EXISTS idx_memberships_company ON association_memberships (company_id);
CREATE INDEX IF NOT EXISTS idx_memberships_tier ON association_memberships (membership_tier);

-- =============================================================================
-- CONTACTS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS contacts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Contact info
    full_name VARCHAR(200) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    title VARCHAR(200),
    department VARCHAR(100),
    seniority VARCHAR(50),
    
    -- Communication
    email VARCHAR(255),
    email_verified BOOLEAN DEFAULT FALSE,
    email_verified_at TIMESTAMP WITH TIME ZONE,
    phone VARCHAR(50),
    linkedin_url VARCHAR(500),
    
    -- Metadata
    data_source VARCHAR(100),
    confidence_score DECIMAL(3, 2),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_contact_email UNIQUE (company_id, email)
);

-- Indexes for contacts
CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts (company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_title_trgm ON contacts USING gin(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts (email);

-- =============================================================================
-- EXTRACTION JOBS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS extraction_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Job info
    job_type VARCHAR(50) NOT NULL,  -- full_extract, incremental, enrichment, validation
    association_code VARCHAR(20),
    
    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, running, completed, failed, cancelled
    
    -- Progress
    total_items INTEGER DEFAULT 0,
    processed_items INTEGER DEFAULT 0,
    created_items INTEGER DEFAULT 0,
    updated_items INTEGER DEFAULT 0,
    failed_items INTEGER DEFAULT 0,
    skipped_items INTEGER DEFAULT 0,
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Checkpoints
    last_checkpoint JSONB,
    checkpoint_at TIMESTAMP WITH TIME ZONE,
    
    -- Errors
    error_count INTEGER DEFAULT 0,
    error_log JSONB DEFAULT '[]'::jsonb,
    last_error TEXT,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for jobs
CREATE INDEX IF NOT EXISTS idx_jobs_status ON extraction_jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_association ON extraction_jobs (association_code);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON extraction_jobs (created_at DESC);

-- =============================================================================
-- QUALITY AUDIT LOG TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS quality_audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    job_id UUID REFERENCES extraction_jobs(id) ON DELETE SET NULL,
    
    -- Change details
    field_name VARCHAR(100) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    
    -- Validation
    validation_result VARCHAR(20),  -- passed, failed, warning, corrected
    validator_name VARCHAR(100),
    confidence_score DECIMAL(3, 2),
    
    -- Metadata
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for audit log
CREATE INDEX IF NOT EXISTS idx_audit_company ON quality_audit_log (company_id);
CREATE INDEX IF NOT EXISTS idx_audit_job ON quality_audit_log (job_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON quality_audit_log (created_at DESC);

-- =============================================================================
-- URL QUEUE TABLE (for crawler state)
-- =============================================================================
CREATE TABLE IF NOT EXISTS url_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID REFERENCES extraction_jobs(id) ON DELETE CASCADE,
    
    -- URL info
    url VARCHAR(2000) NOT NULL,
    url_hash VARCHAR(64) NOT NULL,  -- SHA-256 hash for deduplication
    association_code VARCHAR(20),
    
    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, processing, completed, failed, skipped
    priority INTEGER DEFAULT 0,
    
    -- Metadata
    source_url VARCHAR(2000),
    depth INTEGER DEFAULT 0,
    
    -- Processing
    attempts INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT unique_url_per_job UNIQUE (job_id, url_hash)
);

-- Indexes for URL queue
CREATE INDEX IF NOT EXISTS idx_queue_job_status ON url_queue (job_id, status);
CREATE INDEX IF NOT EXISTS idx_queue_priority ON url_queue (priority DESC, created_at ASC);

-- =============================================================================
-- DUPLICATE GROUPS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS duplicate_groups (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Canonical record
    canonical_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,

    -- Group members (array of company IDs before merge)
    member_company_ids UUID[] NOT NULL,

    -- Match details
    match_score DECIMAL(3, 2),
    match_method VARCHAR(50),  -- exact_domain, fuzzy_name, address_match

    -- Status
    status VARCHAR(20) DEFAULT 'merged',  -- pending, merged, rejected
    reviewed_by VARCHAR(100),
    reviewed_at TIMESTAMP WITH TIME ZONE,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for duplicate groups
CREATE INDEX IF NOT EXISTS idx_duplicates_canonical ON duplicate_groups (canonical_company_id);

-- =============================================================================
-- EVENTS TABLE (NEW)
-- =============================================================================
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Core identification
    title VARCHAR(500) NOT NULL,
    event_type VARCHAR(50) DEFAULT 'OTHER',  -- CONFERENCE, TRADE_SHOW, WEBINAR, etc.
    description TEXT,

    -- Dates
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    registration_deadline TIMESTAMP WITH TIME ZONE,

    -- Location
    venue VARCHAR(300),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100) DEFAULT 'United States',
    is_virtual BOOLEAN DEFAULT FALSE,

    -- URLs
    event_url VARCHAR(500),
    registration_url VARCHAR(500),

    -- Organizer
    organizer_name VARCHAR(200),
    organizer_association VARCHAR(20),

    -- Participant counts
    expected_attendees INTEGER,
    exhibitor_count INTEGER,
    sponsor_count INTEGER,

    -- Provenance
    source_url VARCHAR(500),
    extracted_by VARCHAR(100),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for events
CREATE INDEX IF NOT EXISTS idx_events_dates ON events (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_association ON events (organizer_association);
CREATE INDEX IF NOT EXISTS idx_events_location ON events (state, city);

-- =============================================================================
-- EVENT PARTICIPANTS TABLE (NEW)
-- =============================================================================
CREATE TABLE IF NOT EXISTS event_participants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Links
    event_id UUID REFERENCES events(id) ON DELETE CASCADE,
    company_id UUID REFERENCES companies(id) ON DELETE SET NULL,

    -- Participant info
    participant_type VARCHAR(20) NOT NULL,  -- SPONSOR, EXHIBITOR, ATTENDEE, SPEAKER
    company_name VARCHAR(500) NOT NULL,
    company_website VARCHAR(500),

    -- Sponsor-specific
    sponsor_tier VARCHAR(20),  -- PLATINUM, GOLD, SILVER, BRONZE, etc.

    -- Exhibitor-specific
    booth_number VARCHAR(20),
    booth_category VARCHAR(100),

    -- Speaker-specific
    speaker_name VARCHAR(200),
    speaker_title VARCHAR(200),
    presentation_title VARCHAR(500),

    -- Provenance
    source_url VARCHAR(500),
    extracted_by VARCHAR(100),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for event participants
CREATE INDEX IF NOT EXISTS idx_participants_event ON event_participants (event_id);
CREATE INDEX IF NOT EXISTS idx_participants_company ON event_participants (company_id);
CREATE INDEX IF NOT EXISTS idx_participants_type ON event_participants (participant_type);
CREATE INDEX IF NOT EXISTS idx_participants_tier ON event_participants (sponsor_tier);

-- =============================================================================
-- COMPETITOR SIGNALS TABLE (NEW)
-- =============================================================================
CREATE TABLE IF NOT EXISTS competitor_signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Competitor identification
    competitor_name VARCHAR(100) NOT NULL,
    competitor_normalized VARCHAR(100),

    -- Signal details
    signal_type VARCHAR(50) NOT NULL,  -- SPONSOR, EXHIBITOR, MEMBER_USAGE, etc.
    context TEXT NOT NULL,
    confidence DECIMAL(3, 2) DEFAULT 0.80,

    -- Related entities
    source_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    source_event_id UUID REFERENCES events(id) ON DELETE SET NULL,
    source_association VARCHAR(20),

    -- Provenance
    source_url VARCHAR(500),
    extracted_by VARCHAR(100),

    -- Timestamps
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for competitor signals
CREATE INDEX IF NOT EXISTS idx_signals_competitor ON competitor_signals (competitor_normalized);
CREATE INDEX IF NOT EXISTS idx_signals_type ON competitor_signals (signal_type);
CREATE INDEX IF NOT EXISTS idx_signals_company ON competitor_signals (source_company_id);
CREATE INDEX IF NOT EXISTS idx_signals_event ON competitor_signals (source_event_id);
CREATE INDEX IF NOT EXISTS idx_signals_detected ON competitor_signals (detected_at DESC);

-- =============================================================================
-- ENTITY RELATIONSHIPS TABLE (NEW)
-- =============================================================================
CREATE TABLE IF NOT EXISTS entity_relationships (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Relationship
    source_id UUID NOT NULL,
    source_type VARCHAR(20) NOT NULL,  -- Association, Company, Event, Person, Competitor
    target_id UUID NOT NULL,
    target_type VARCHAR(20) NOT NULL,
    relationship_type VARCHAR(50) NOT NULL,  -- ASSOCIATION_HAS_MEMBER, EVENT_HAS_SPONSOR, etc.

    -- Metadata
    properties JSONB DEFAULT '{}'::jsonb,
    confidence DECIMAL(3, 2) DEFAULT 1.00,

    -- Provenance
    source_url VARCHAR(500),
    extracted_by VARCHAR(100),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Constraints
    CONSTRAINT unique_relationship UNIQUE (source_id, target_id, relationship_type)
);

-- Indexes for relationships
CREATE INDEX IF NOT EXISTS idx_relationships_source ON entity_relationships (source_id, source_type);
CREATE INDEX IF NOT EXISTS idx_relationships_target ON entity_relationships (target_id, target_type);
CREATE INDEX IF NOT EXISTS idx_relationships_type ON entity_relationships (relationship_type);

-- =============================================================================
-- SOURCE BASELINES TABLE (NEW)
-- =============================================================================
CREATE TABLE IF NOT EXISTS source_baselines (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Source identification
    url VARCHAR(2000) NOT NULL,
    url_hash VARCHAR(64) NOT NULL,
    domain VARCHAR(255),

    -- DOM structure
    selector_hashes JSONB DEFAULT '{}'::jsonb,
    page_structure_hash VARCHAR(64),

    -- Content indicators
    expected_item_count INTEGER,
    content_hash VARCHAR(64),

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_checked_at TIMESTAMP WITH TIME ZONE,
    last_changed_at TIMESTAMP WITH TIME ZONE,
    change_count INTEGER DEFAULT 0,

    -- Alert configuration
    alert_on_change BOOLEAN DEFAULT TRUE,
    alert_threshold INTEGER DEFAULT 1,  -- Number of changes before alert

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Constraints
    CONSTRAINT unique_baseline_url UNIQUE (url_hash)
);

-- Indexes for source baselines
CREATE INDEX IF NOT EXISTS idx_baselines_domain ON source_baselines (domain);
CREATE INDEX IF NOT EXISTS idx_baselines_active ON source_baselines (is_active);
CREATE INDEX IF NOT EXISTS idx_baselines_checked ON source_baselines (last_checked_at DESC);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers
DROP TRIGGER IF EXISTS update_companies_updated_at ON companies;
CREATE TRIGGER update_companies_updated_at
    BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_memberships_updated_at ON association_memberships;
CREATE TRIGGER update_memberships_updated_at
    BEFORE UPDATE ON association_memberships
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_contacts_updated_at ON contacts;
CREATE TRIGGER update_contacts_updated_at
    BEFORE UPDATE ON contacts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_jobs_updated_at ON extraction_jobs;
CREATE TRIGGER update_jobs_updated_at
    BEFORE UPDATE ON extraction_jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- VIEWS
-- =============================================================================

-- View: Company summary with latest membership
CREATE OR REPLACE VIEW company_summary AS
SELECT 
    c.id,
    c.canonical_name,
    c.domain,
    c.website,
    c.city,
    c.state,
    c.employee_count_min,
    c.employee_count_max,
    c.revenue_min_usd,
    c.erp_system,
    c.quality_score,
    c.quality_grade,
    array_agg(DISTINCT m.association_code) as associations,
    count(DISTINCT ct.id) as contact_count,
    c.last_verified_at,
    c.updated_at
FROM companies c
LEFT JOIN association_memberships m ON c.id = m.company_id
LEFT JOIN contacts ct ON c.id = ct.company_id
GROUP BY c.id;

-- View: Extraction job summary
CREATE OR REPLACE VIEW job_summary AS
SELECT
    j.id,
    j.job_type,
    j.association_code,
    j.status,
    j.total_items,
    j.processed_items,
    CASE WHEN j.total_items > 0
         THEN ROUND((j.processed_items::decimal / j.total_items) * 100, 1)
         ELSE 0 END as progress_percent,
    j.created_items,
    j.failed_items,
    j.error_count,
    EXTRACT(EPOCH FROM (COALESCE(j.completed_at, NOW()) - j.started_at)) as duration_seconds,
    j.started_at,
    j.completed_at
FROM extraction_jobs j
ORDER BY j.created_at DESC;

-- View: Event summary with participant counts
CREATE OR REPLACE VIEW event_summary AS
SELECT
    e.id,
    e.title,
    e.event_type,
    e.start_date,
    e.end_date,
    e.venue,
    e.city,
    e.state,
    e.is_virtual,
    e.organizer_association,
    e.event_url,
    COUNT(DISTINCT ep.id) FILTER (WHERE ep.participant_type = 'SPONSOR') as sponsor_count,
    COUNT(DISTINCT ep.id) FILTER (WHERE ep.participant_type = 'EXHIBITOR') as exhibitor_count,
    COUNT(DISTINCT ep.id) FILTER (WHERE ep.participant_type = 'SPEAKER') as speaker_count,
    COUNT(DISTINCT ep.id) as total_participants,
    e.created_at,
    e.updated_at
FROM events e
LEFT JOIN event_participants ep ON e.id = ep.event_id
GROUP BY e.id
ORDER BY e.start_date DESC;

-- View: Competitor signal report
CREATE OR REPLACE VIEW competitor_report AS
SELECT
    cs.competitor_normalized as competitor,
    cs.signal_type,
    COUNT(*) as signal_count,
    COUNT(DISTINCT cs.source_company_id) as companies_using,
    COUNT(DISTINCT cs.source_event_id) as events_present,
    AVG(cs.confidence) as avg_confidence,
    MAX(cs.detected_at) as last_detected
FROM competitor_signals cs
GROUP BY cs.competitor_normalized, cs.signal_type
ORDER BY signal_count DESC;

-- =============================================================================
-- SEED DATA
-- =============================================================================

-- Insert association reference data
INSERT INTO association_memberships (id, company_id, association_code, association_name)
SELECT 
    uuid_generate_v4(),
    NULL,
    code,
    name
FROM (VALUES
    ('PMA', 'Precision Metalforming Association'),
    ('NEMA', 'National Electrical Manufacturers Association'),
    ('SOCMA', 'Society of Chemical Manufacturers & Affiliates'),
    ('AIA', 'Aerospace Industries Association'),
    ('AGMA', 'American Gear Manufacturers Association'),
    ('NTMA', 'National Tooling & Machining Association'),
    ('PMPA', 'Precision Machined Products Association'),
    ('FIA', 'Forging Industry Association'),
    ('NADCA', 'North American Die Casting Association'),
    ('AFS', 'American Foundry Society')
) AS t(code, name)
ON CONFLICT DO NOTHING;

COMMIT;
"""

DROP_TABLES_SQL = """
DROP VIEW IF EXISTS company_summary CASCADE;
DROP VIEW IF EXISTS job_summary CASCADE;
DROP VIEW IF EXISTS event_summary CASCADE;
DROP VIEW IF EXISTS competitor_report CASCADE;
DROP TABLE IF EXISTS source_baselines CASCADE;
DROP TABLE IF EXISTS entity_relationships CASCADE;
DROP TABLE IF EXISTS competitor_signals CASCADE;
DROP TABLE IF EXISTS event_participants CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS quality_audit_log CASCADE;
DROP TABLE IF EXISTS duplicate_groups CASCADE;
DROP TABLE IF EXISTS url_queue CASCADE;
DROP TABLE IF EXISTS contacts CASCADE;
DROP TABLE IF EXISTS association_memberships CASCADE;
DROP TABLE IF EXISTS extraction_jobs CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
"""


def get_connection():
    """Get database connection from environment."""
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    return psycopg2.connect(database_url)


def create_database_if_not_exists():
    """Create the database if it doesn't exist."""
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    # Parse database name from URL
    # Format: postgresql://user:pass@host:port/dbname
    db_name = database_url.split("/")[-1].split("?")[0]
    base_url = database_url.rsplit("/", 1)[0] + "/postgres"
    
    conn = psycopg2.connect(base_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cur.fetchone()
        
        if not exists:
            print(f"Creating database: {db_name}")
            cur.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database {db_name} created successfully")
        else:
            print(f"Database {db_name} already exists")
    
    conn.close()


def init_database(drop_existing: bool = False):
    """Initialize database tables."""
    
    # Create database if needed
    create_database_if_not_exists()
    
    conn = get_connection()
    
    try:
        with conn.cursor() as cur:
            if drop_existing:
                print("Dropping existing tables...")
                cur.execute(DROP_TABLES_SQL)
                conn.commit()
                print("Existing tables dropped")
            
            print("Creating tables...")
            cur.execute(CREATE_TABLES_SQL)
            conn.commit()
            print("Tables created successfully")
            
            # Verify tables
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = cur.fetchall()
            
            print(f"\nCreated {len(tables)} tables:")
            for table in tables:
                print(f"  - {table[0]}")
                
    except Exception as e:
        conn.rollback()
        print(f"Error initializing database: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize NAM Intelligence database")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables first")
    parser.add_argument("--force", action="store_true", help="Skip confirmation for drop")
    
    args = parser.parse_args()
    
    if args.drop and not args.force:
        confirm = input("This will DELETE ALL DATA. Type 'yes' to confirm: ")
        if confirm.lower() != "yes":
            print("Aborted")
            sys.exit(0)
    
    init_database(drop_existing=args.drop)
    print("\nDatabase initialization complete!")
