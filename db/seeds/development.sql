-- Local development only. Production tenants are provisioned by the control plane.
INSERT INTO tenants (id, key, name, default_locale)
VALUES (
    '10000000-0000-4000-8000-000000000001',
    'ainexus.local',
    'AI-Nexus Local Development',
    'zh-CN'
)
ON CONFLICT (id) DO NOTHING;
