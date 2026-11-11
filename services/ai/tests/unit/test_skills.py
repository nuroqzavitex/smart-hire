from ai_service.domain.skills import ALIASES, find_skills, normalize, unaccent


def test_normalize() -> None:
    assert normalize(["golang", " K8S ", "postgres", "Go", "", "x" * 51, "Rust"]) == [
        "Go",
        "Kubernetes",
        "PostgreSQL",
        "Rust",
    ]
    assert len(normalize(f"s{i}" for i in range(80))) == 50


def test_normalize_collapses_whitespace_and_dedups_case_insensitively() -> None:
    assert normalize(
        ["  machine \t  learning ", "Machine Learning", "node.js", "NodeJS", "c#", "CSharp"]
    ) == [
        "machine learning",
        "Node.js",
        "C#",
    ]


def test_normalize_keeps_first_spelling_of_unknown_skill() -> None:
    assert normalize(["Terraform", "terraform", "TERRAFORM"]) == ["Terraform"]


def test_normalize_drops_whitespace_only_and_counts_after_dedup() -> None:
    skills = ["   ", "Go", "golang"] + [f"s{i}" for i in range(60)]
    got = normalize(skills)
    assert got[0] == "Go"
    assert got[1] == "s0"
    assert len(got) == 50


def test_aliases_minimum_table() -> None:
    want = {
        "go": "Go",
        "golang": "Go",
        "k8s": "Kubernetes",
        "kubernetes": "Kubernetes",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "psql": "PostgreSQL",
        "js": "JavaScript",
        "javascript": "JavaScript",
        "ts": "TypeScript",
        "typescript": "TypeScript",
        "python": "Python",
        "docker": "Docker",
        "kafka": "Kafka",
        "apache kafka": "Kafka",
        "redis": "Redis",
        "react": "React",
        "reactjs": "React",
        "react.js": "React",
        "node": "Node.js",
        "nodejs": "Node.js",
        "node.js": "Node.js",
        "java": "Java",
        "c++": "C++",
        "cpp": "C++",
        "c#": "C#",
        "csharp": "C#",
        "aws": "AWS",
        "amazon web services": "AWS",
        "gcp": "GCP",
        "google cloud": "GCP",
        "sql": "SQL",
        "grpc": "gRPC",
        "linux": "Linux",
        "git": "Git",
        "mysql": "MySQL",
        "mongodb": "MongoDB",
        "mongo": "MongoDB",
        "rest": "REST",
        "restful": "REST",
        "microservices": "Microservices",
        "microservice": "Microservices",
    }
    for alias, canonical in want.items():
        assert ALIASES[alias] == canonical, alias
    assert all(k == k.lower() for k in ALIASES)


def test_unaccent() -> None:
    assert unaccent("Đà Nẵng Kỹ sư") == "Da Nang Ky su"
    assert unaccent("đường ĐI") == "duong DI"


def test_find_skills_word_boundaries() -> None:
    assert find_skills("Kỹ năng: Golang, k8s, PostgreSQL, C#, C++ và gỗ") == [
        "Go",
        "Kubernetes",
        "PostgreSQL",
        "C#",
        "C++",
    ]
    assert find_skills("mongoose gopher") == []


def test_find_skills_short_aliases_ignored_and_order_of_appearance() -> None:
    # "go", "js", "ts" are shorter than 3 letters and must not match on their own.
    assert find_skills("I go to js and ts meetups") == []
    assert find_skills("Docker, then Apache Kafka; later node.js and Kafka again") == [
        "Docker",
        "Kafka",
        "Node.js",
    ]


def test_find_skills_does_not_match_inside_identifiers() -> None:
    assert find_skills("javascripts restore gitlab sqlite") == []
    assert find_skills("React.js/TypeScript (gRPC)") == ["React", "TypeScript", "gRPC"]
