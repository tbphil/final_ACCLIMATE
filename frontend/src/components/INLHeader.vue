<template>
  <header class="header d-flex align-items-center justify-content-between">
    <!-- Left Logo -->
    <img
      src="@/assets/INL-Logo_Left-White.png"
      alt="Left Logo"
      class="header-logo"
    />

    <!-- Middle: "Extreme Weather" | "ACCLIMATE" | "Critical Infrastructure" -->
    <div class="header-center d-flex align-items-center justify-content-center">
      <!-- "Extreme Weather" emits open-climate -->
      <span class="header-word" @click="$emit('open-climate')">
        Extreme Weather
      </span>

      <!-- Pipe -->
      <span class="mx-3">|</span>

      <!-- "ACCLIMATE" emits open-hbom -->
      <span class="header-acclimate" @click="$emit('open-hbom')">
        ACCLIMATE
      </span>

      <!-- Pipe -->
      <span class="mx-3">|</span>

      <!-- "Critical Infrastructure" emits open-infrastructure -->
      <span class="header-word" 
        :class="{ 'disabled-link': !climateLoaded }"
        @click="$emit('open-infrastructure')">
        Critical Infrastructure
      </span>
    </div>
    <div style="width: 60px;"></div>
  </header>
</template>

<script>
export default {
  name: 'INLHeader',
  props: {
    climateLoaded: {
      type: Boolean,
      default: false
    }
  }
};
</script>