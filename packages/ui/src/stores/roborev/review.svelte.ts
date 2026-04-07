import type {
  RoborevClient,
} from "../../api/roborev/client.js";
import type {
  components,
} from "../../api/roborev/generated/schema.js";

type Review = components["schemas"]["Review"];
type ReviewResponse = components["schemas"]["Response"];

export interface ReviewStoreOptions {
  client: RoborevClient;
}

export function createReviewStore(
  opts: ReviewStoreOptions,
) {
  const client = opts.client;

  // State
  let review = $state<Review | null>(null);
  let responses = $state<ReviewResponse[]>([]);
  let loading = $state(false);
  let selectedJobId = $state<number | undefined>(
    undefined,
  );
  let reviewNotFound = $state(false);
  let requestVersion = 0;

  async function loadReview(
    jobId: number,
  ): Promise<void> {
    const version = ++requestVersion;
    loading = true;
    reviewNotFound = false;
    review = null;
    responses = [];
    try {
      const [reviewResult, commentsResult] =
        await Promise.all([
          client.GET("/api/review", {
            params: { query: { job_id: jobId } },
          }),
          client.GET("/api/comments", {
            params: { query: { job_id: jobId } },
          }),
        ]);

      if (version !== requestVersion) return;

      if (reviewResult.error) {
        review = null;
        reviewNotFound = true;
      } else {
        review = reviewResult.data ?? null;
        reviewNotFound = false;
      }

      if (
        !commentsResult.error &&
        commentsResult.data
      ) {
        responses =
          commentsResult.data.responses ?? [];
      }
    } catch {
      if (version !== requestVersion) return;
      review = null;
      reviewNotFound = true;
    } finally {
      if (version === requestVersion) loading = false;
    }
  }

  async function closeReview(
    jobId: number,
  ): Promise<void> {
    const closed = !(review?.closed ?? false);
    const { error } = await client.POST(
      "/api/review/close",
      { body: { job_id: jobId, closed } },
    );
    if (error) return;
    if (review) {
      review = { ...review, closed };
    }
  }

  async function addComment(
    jobId: number,
    text: string,
  ): Promise<boolean> {
    const { data, error } = await client.POST(
      "/api/comment",
      {
        body: {
          job_id: jobId,
          commenter: "web",
          comment: text,
        },
      },
    );
    if (error || !data) return false;
    responses = [...responses, data];
    return true;
  }

  function setSelectedJobId(
    jobId: number | undefined,
  ): void {
    selectedJobId = jobId;
    if (jobId !== undefined) {
      void loadReview(jobId);
    } else {
      review = null;
      responses = [];
      reviewNotFound = false;
    }
  }

  // Getters
  function getReview(): Review | null {
    return review;
  }
  function getResponses(): ReviewResponse[] {
    return responses;
  }
  function isLoading(): boolean {
    return loading;
  }
  function getSelectedJobId(): number | undefined {
    return selectedJobId;
  }
  function isReviewNotFound(): boolean {
    return reviewNotFound;
  }
  function getPrompt(): string {
    return review?.prompt ?? "";
  }
  function getOutput(): string {
    return review?.output ?? "";
  }
  function isClosed(): boolean {
    return review?.closed ?? false;
  }

  return {
    getReview,
    getResponses,
    isLoading,
    getSelectedJobId,
    isReviewNotFound,
    getPrompt,
    getOutput,
    isClosed,
    loadReview,
    closeReview,
    addComment,
    setSelectedJobId,
  };
}

export type ReviewStore = ReturnType<
  typeof createReviewStore
>;
