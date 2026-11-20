from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScoreBreakdown(_message.Message):
    __slots__ = ("semantic_similarity", "required_skill_coverage", "experience_fit", "location_fit", "matched_skills", "missing_skills")
    SEMANTIC_SIMILARITY_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_SKILL_COVERAGE_FIELD_NUMBER: _ClassVar[int]
    EXPERIENCE_FIT_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIT_FIELD_NUMBER: _ClassVar[int]
    MATCHED_SKILLS_FIELD_NUMBER: _ClassVar[int]
    MISSING_SKILLS_FIELD_NUMBER: _ClassVar[int]
    semantic_similarity: float
    required_skill_coverage: float
    experience_fit: float
    location_fit: float
    matched_skills: _containers.RepeatedScalarFieldContainer[str]
    missing_skills: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, semantic_similarity: _Optional[float] = ..., required_skill_coverage: _Optional[float] = ..., experience_fit: _Optional[float] = ..., location_fit: _Optional[float] = ..., matched_skills: _Optional[_Iterable[str]] = ..., missing_skills: _Optional[_Iterable[str]] = ...) -> None: ...

class ScoreMatchRequest(_message.Message):
    __slots__ = ("cv_id", "job_id")
    CV_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    cv_id: str
    job_id: str
    def __init__(self, cv_id: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class ScoreMatchResponse(_message.Message):
    __slots__ = ("score", "breakdown", "model_version")
    SCORE_FIELD_NUMBER: _ClassVar[int]
    BREAKDOWN_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    score: float
    breakdown: ScoreBreakdown
    model_version: str
    def __init__(self, score: _Optional[float] = ..., breakdown: _Optional[_Union[ScoreBreakdown, _Mapping]] = ..., model_version: _Optional[str] = ...) -> None: ...

class FindCandidatesRequest(_message.Message):
    __slots__ = ("job_id", "top_k")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    top_k: int
    def __init__(self, job_id: _Optional[str] = ..., top_k: _Optional[int] = ...) -> None: ...

class FindCandidatesResponse(_message.Message):
    __slots__ = ("candidates", "model_version")
    CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    candidates: _containers.RepeatedCompositeFieldContainer[RankedCandidate]
    model_version: str
    def __init__(self, candidates: _Optional[_Iterable[_Union[RankedCandidate, _Mapping]]] = ..., model_version: _Optional[str] = ...) -> None: ...

class RankedCandidate(_message.Message):
    __slots__ = ("candidate_id", "cv_id", "score", "breakdown")
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    CV_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    BREAKDOWN_FIELD_NUMBER: _ClassVar[int]
    candidate_id: str
    cv_id: str
    score: float
    breakdown: ScoreBreakdown
    def __init__(self, candidate_id: _Optional[str] = ..., cv_id: _Optional[str] = ..., score: _Optional[float] = ..., breakdown: _Optional[_Union[ScoreBreakdown, _Mapping]] = ...) -> None: ...

class RecommendJobsRequest(_message.Message):
    __slots__ = ("cv_id", "top_k")
    CV_ID_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    cv_id: str
    top_k: int
    def __init__(self, cv_id: _Optional[str] = ..., top_k: _Optional[int] = ...) -> None: ...

class RecommendJobsResponse(_message.Message):
    __slots__ = ("jobs", "model_version")
    JOBS_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[RankedJob]
    model_version: str
    def __init__(self, jobs: _Optional[_Iterable[_Union[RankedJob, _Mapping]]] = ..., model_version: _Optional[str] = ...) -> None: ...

class RankedJob(_message.Message):
    __slots__ = ("job_id", "score", "breakdown")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    BREAKDOWN_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    score: float
    breakdown: ScoreBreakdown
    def __init__(self, job_id: _Optional[str] = ..., score: _Optional[float] = ..., breakdown: _Optional[_Union[ScoreBreakdown, _Mapping]] = ...) -> None: ...
